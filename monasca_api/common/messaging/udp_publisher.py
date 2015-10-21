# Copyright 2015 Telefónica I+D
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
import socket

from urlparse import urlparse

from oslo_config import cfg
from oslo_log import log

from monasca_api.common.messaging import publisher

LOG = log.getLogger(__name__)


class UdpPublisher(publisher.Publisher):
    def __init__(self):
        if not cfg.CONF.udp.uri:
            raise Exception('UDP Publisher is not configured correctly! '
                            'Use configuration file to specify endpoint '
                            '(for example: uri=localhost:5000)')

        self.uri = cfg.CONF.udp.uri

        parsed_uri = urlparse('//' + self.uri)
        self.udp_address = (parsed_uri.hostname, parsed_uri.port)
        self.udp_socket = None

    def _init_socket(self):
        try:
            if not self.udp_socket:
                self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            LOG.debug('UDP socket was created successfully.')
        except Exception:
            self.udp_socket = None
            LOG.exception('UDP socket can not be created.')

    def close(self):
        if self.udp_socket:
            self.udp_socket.close()
            self.udp_socket = None

    def send_message(self, message):
        try:
            if not self.udp_socket:
                self._init_socket()
            self.udp_socket.sendto(json.dumps(message), self.udp_address)

        except IOError as ex:
            LOG.exception('Error occurred while forwarding data to {}: {}.'.format(self.uri, ex))
        except Exception:
            LOG.exception('Unknown error.')

    def send_message_batch(self, messages):
        for message in messages:
            self.send_message(message)
