import pytest
import os
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=["configs/first.crt", "configs/first.key",
                                                  "configs/second.crt", "configs/second.key",
                                                  "configs/cert.xml"])

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def change_config_to_key(name):
    '''
      * Generate config with certificate/key name from args.
      * Reload config.
    '''
    node.exec_in_container(["bash", "-c" , """cat > /etc/clickhouse-server/config.d/cert.xml << EOF
<?xml version="1.0"?>
<clickhouse>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/config.d/{cur_name}.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/config.d/{cur_name}.key</privateKeyFile>
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
        </server>
    </openSSL>
</clickhouse>
EOF""".format(cur_name=name)])
    node.query("SYSTEM RELOAD CONFIG")

def test_first_than_second_cert():
    ''' Consistently set first key and check that only it will be accepted, then repeat same for second key. '''
    # Set first key
    change_config_to_key('first')

    # Command with correct certificate
    assert node.exec_in_container(['curl', '--silent', '--cacert', '/etc/clickhouse-server/config.d/{cur_name}.crt'.format(cur_name='first'),
                            'https://localhost:8443/']) == 'Ok.\n'

    # Command with wrong certificate
    # This command don't use option '-k', so it will lead to error while execution. 
    # That's why except will always work
    try:
        node.exec_in_container(['curl', '--silent', '--cacert', '/etc/clickhouse-server/config.d/{cur_name}.crt'.format(cur_name='second'),
                            'https://localhost:8443/'])
        assert False
    except:
        assert True
    
    # Change to other key
    change_config_to_key('second')

    # Command with correct certificate
    assert node.exec_in_container(['curl', '--silent', '--cacert', '/etc/clickhouse-server/config.d/{cur_name}.crt'.format(cur_name='second'),
                            'https://localhost:8443/']) == 'Ok.\n'

    # Command with wrong certificate
    # Same as previous
    try:
        node.exec_in_container(['curl', '--silent', '--cacert', '/etc/clickhouse-server/config.d/{cur_name}.crt'.format(cur_name='first'),
                            'https://localhost:8443/'])
        assert False
    except:
        assert True
