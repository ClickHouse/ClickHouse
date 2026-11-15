import os
import threading

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

TEST_DIR = os.path.dirname(__file__)

cluster = ClickHouseCluster(
    __file__,
    zookeeper_certfile=os.path.join(TEST_DIR, "configs_secure", "first_client.crt"),
    zookeeper_keyfile=os.path.join(TEST_DIR, "configs_secure", "first_client.key"),
)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs_secure/first_client.crt",
        "configs_secure/first_client.key",
        "configs_secure/second_client.crt",
        "configs_secure/second_client.key",
        "configs_secure/third_client.crt",
        "configs_secure/third_client.key",
        "configs_secure/conf.d/remote_servers.xml",
        "configs_secure/conf.d/ssl_conf.xml",
        "configs/zookeeper_config_with_ssl.xml",
    ],
    with_zookeeper_secure=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs_secure/first_client.crt",
        "configs_secure/first_client.key",
        "configs_secure/second_client.crt",
        "configs_secure/second_client.key",
        "configs_secure/third_client.crt",
        "configs_secure/third_client.key",
        "configs_secure/conf.d/remote_servers.xml",
        "configs_secure/conf.d/ssl_conf.xml",
        "configs/zookeeper_config_with_ssl.xml",
    ],
    with_zookeeper_secure=True,
)

nodes = [node1, node2]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def secure_connection_test(started_cluster):
    # No asserts, connection works

    def check_node(node):
        assert node.query('SELECT COUNT() > 0 FROM system.zookeeper_connection WHERE NOT is_expired') == '1\n'

    for node in nodes:
        check_node(node)

    threads_number = 4
    iterations = 4
    threads = []

    # Just checking for race conditions

    for _ in range(threads_number):
        threads.append(
            threading.Thread(
                target=lambda: [
                    check_node(node)
                    for node in nodes
                    for _ in range(iterations)
                ]
            )
        )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def change_config_to_key(name):
    """
    Generate config with certificate/key name from args.
    Reload config.
    """
    for node in nodes:
        node.exec_in_container(
            [
                "bash",
                "-c",
                """cat > /etc/clickhouse-server/config.d/ssl_conf.xml << EOF
<clickhouse>
    <openSSL>
        <client>
            <certificateFile>/etc/clickhouse-server/config.d/{cur_name}_client.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/config.d/{cur_name}_client.key</privateKeyFile>
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
            <verificationMode>none</verificationMode>
            <invalidCertificateHandler>
                <name>RejectCertificateHandler</name>
            </invalidCertificateHandler>
        </client>
    </openSSL>
</clickhouse>
EOF""".format(
                    cur_name=name
                ),
            ]
        )

        node.query("SYSTEM RELOAD CONFIG")
        node.exec_in_container(["ss", "--kill", "-tn", "state", "established",
                                f"( dport = :{cluster.zookeeper_port} or dport = :{cluster.zookeeper_secure_port} )"])


def assert_zookeeper_connection(success):
    if success:
        secure_connection_test(started_cluster)
    else:
        try:
            secure_connection_test(started_cluster)
            assert False
        except QueryRuntimeException as e:
            assert 'ssl/tls alert certificate unknown' in e.stderr


def test_wrong_cn_cert():
    """Checking the certificate reload with an incorrect CN, the expected behavior is Code: 210."""
    change_config_to_key("first")
    assert_zookeeper_connection(True)
    change_config_to_key("second")
    assert_zookeeper_connection(False)


def test_correct_cn_cert():
    """Replacement with a valid certificate, the expected behavior is to restore the connection with Zookeeper."""
    change_config_to_key("second")
    assert_zookeeper_connection(False)
    change_config_to_key("third")
    assert_zookeeper_connection(True)
