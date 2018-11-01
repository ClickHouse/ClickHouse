import pytest

import os
import random
import signal
import subprocess
import tempfile
import threading
import time


# TODO: also support stateful queries.
QUERIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries', '0_stateless')

SERVER_CONFIG = \
"""\
<?xml version="1.0"?>
<!-- Config for test server -->
<yandex>
    <logger>
        <level>trace</level>
        <log>{log_dir}/clickhouse-server.log</log>
        <errorlog>{log_dir}/clickhouse-server.err.log</errorlog>
        <size>10M</size>
        <count>1</count>
        <compress>0</compress>
    </logger>
    <listen_host>::</listen_host>
    <listen_host>0.0.0.0</listen_host>
    <listen_try>1</listen_try>
    <openSSL>
        <server> <!-- Used for https server AND secure tcp port -->
            <certificateFile>{etc_dir}/server.crt</certificateFile>
            <privateKeyFile>{etc_dir}/server.key</privateKeyFile>
            <dhParamsFile>{etc_dir}/dhparam.pem</dhParamsFile>
            <verificationMode>none</verificationMode>
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
        </server>

        <client> <!-- Used for connecting to https dictionary source -->
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
            <verificationMode>none</verificationMode>
            <invalidCertificateHandler>
                <name>AcceptCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>

    <keep_alive_timeout>3</keep_alive_timeout>
    <path>{tmp_dir}/data/</path>
    <tmp_path>{tmp_dir}/tmp/</tmp_path>
    <users_config>users.xml</users_config>
    <mark_cache_size>5368709120</mark_cache_size>
    <default_profile>default</default_profile>
    <default_database>default</default_database>
    <timezone>Europe/Moscow</timezone>
    <remote_servers incl="clickhouse_remote_servers" >
        <!-- Test only shard config for testing distributed storage -->
        <test_shard_localhost>
            <shard>
                <replica>
                    <host>localhost</host>
                    <port>59000</port>
                </replica>
            </shard>
        </test_shard_localhost>
        <test_shard_localhost_secure>
            <shard>
                <replica>
                    <host>localhost</host>
                    <port>59440</port>
                    <secure>1</secure>
                </replica>
            </shard>
        </test_shard_localhost_secure>
    </remote_servers>
    <include_from/>
    <zookeeper incl="zookeeper-servers" optional="true" />
    <macros incl="macros" optional="true" />
    <builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
    <max_session_timeout>3600</max_session_timeout>
    <default_session_timeout>60</default_session_timeout>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>
    <dictionaries_config>*_dictionary.xml</dictionaries_config>
    <compression incl="clickhouse_compression">
    </compression>
    <distributed_ddl>
        <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>
    <format_schema_path>{tmp_dir}/data/format_schemas/</format_schema_path>
</yandex>
"""

USERS_CONFIG = \
"""\
<?xml version="1.0"?>
<yandex>
    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <use_uncompressed_cache>0</use_uncompressed_cache>
            <load_balancing>random</load_balancing>
        </default>

        <readonly>
            <readonly>1</readonly>
        </readonly>
    </profiles>

    <users>
        <default>
            <password></password>
            <networks incl="networks" replace="replace">
                <ip>::/0</ip>
            </networks>

            <profile>default</profile>

            <quota>default</quota>
        </default>

        <readonly>
            <password></password>
            <networks incl="networks" replace="replace">
                <ip>::1</ip>
                <ip>127.0.0.1</ip>
            </networks>
            <profile>readonly</profile>
            <quota>default</quota>
        </readonly>
    </users>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</yandex>
"""


@pytest.fixture(scope='module', params=[f for f in os.listdir(QUERIES_PATH) if f.endswith('.reference')])
def test_case(request):
    return request.param


def test_query(test_case, cmdopts):
    port_base = random.SystemRandom().randrange(10000, 60000)

    tcp_port = port_base + 1
    http_port = port_base + 2
    inter_port = port_base + 3
    tcps_port = port_base + 4
    https_port = port_base + 5
    odbc_port = port_base + 6

    bin_prefix = 'clickhouse'
    if cmdopts['builddir'] is not None:
        bin_prefix = os.path.join(cmdopts['builddir'], 'dbms', 'programs', bin_prefix)

    tmp_dir = tempfile.mkdtemp(prefix='clickhouse.test..')
    log_dir = os.path.join(tmp_dir, 'log')
    etc_dir = os.path.join(tmp_dir, 'etc')
    config_path = os.path.join(etc_dir, 'server-config.xml')
    users_path = os.path.join(etc_dir, 'users.xml')

    os.makedirs(log_dir)
    os.makedirs(etc_dir)

    with open(config_path, 'w') as f:
        f.write(SERVER_CONFIG.format(tmp_dir=tmp_dir, log_dir=log_dir, etc_dir=etc_dir))

    with open(users_path, 'w') as f:
        f.write(USERS_CONFIG)

    class ServerThread(threading.Thread):
        def __init__(self, bin_prefix, args):
            self._bin = bin_prefix + '-server'
            self._args = args
            self._lock = threading.Lock()
            threading.Thread.__init__(self)
            self._lock.acquire()
            self._out = ''
            self._err = ''

        def run(self):
            self._proc = subprocess.Popen([self._bin] + self._args, shell=False, bufsize=1,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            while self._proc.poll() is None:
                line = self._proc.stderr.readline()
                self._err += line
                # if 'Application: Ready for connections' in line:
                break

            # If process has died then try to fetch output before releasing lock
            if self._proc.returncode is not None:
                stdout, stderr = self._proc.communicate()
                if stdout:
                    self._out += stdout
                if stderr:
                    self._err += stderr

            self._lock.release()

            if self._proc.returncode is None:
                stdout, stderr = self._proc.communicate()
                if stdout:
                    self._out += stdout
                if stderr:
                    self._err += stderr

        def wait(self):
            self._lock.acquire()
            if self._proc.returncode is not None:
                return False
            self._lock.release()
            return True

        def stop(self):
            if self._proc.returncode is None:
                self._proc.send_signal(signal.SIGINT)

    print 'Start clickhouse-server with args:'

    server_args = [
        '--config-file={config_path}'.format(config_path=config_path),
        '--',
        '--tcp_port={tcp_port}'.format(tcp_port=tcp_port),
        '--http_port={http_port}'.format(http_port=http_port),
        '--interserver_http_port={inter_port}'.format(inter_port=inter_port),
        '--tcp_port_secure={tcps_port}'.format(tcps_port=tcps_port),
        '--https_port={https_port}'.format(https_port=https_port),
        '--odbc_bridge.port={odbc_port}'.format(odbc_port=odbc_port),
    ]
    print server_args

    server_thread = ServerThread(bin_prefix, server_args)
    server_thread.start()
    if not server_thread.wait():
        print server_thread._out
        print server_thread._err
        pytest.fail('Server died unexpectedly')
        return

    # TODO: run test

    server_thread.stop()
    server_thread.join()

    print server_thread._out
    print server_thread._err

    print 'Stop clickhouse-server'
