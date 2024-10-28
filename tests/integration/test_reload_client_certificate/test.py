import os
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

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

    node1.query("SELECT count() FROM system.zookeeper WHERE path = '/'")
    node2.query("SELECT count() FROM system.zookeeper WHERE path = '/'")

    threads_number = 4
    iterations = 4
    threads = []

    # Just checking for race conditions

    for _ in range(threads_number):
        threads.append(
            threading.Thread(
                target=lambda: [
                    node1.query("SELECT count() FROM system.zookeeper WHERE path = '/'")
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

        node.exec_in_container(
            ["bash", "-c", "touch /etc/clickhouse-server/config.d/ssl_conf.xml"],
        )


def check_reload_successful(node, cert_name):
    return node.grep_in_log(
        f"Reloaded certificate (/etc/clickhouse-server/config.d/{cert_name}_client.crt)"
    )


def check_error_handshake(node):
    return node.count_in_log("Code: 210.")


def clean_logs():
    for node in nodes:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "echo -n > /var/log/clickhouse-server/clickhouse-server.log",
            ]
        )


def drop_secure_zk_connection(pm, node, action="DROP"):
    pm._check_instance(node)
    pm._add_rule(
        {
            "source": node.ip_address,
            "destination_port": 2281,
            "action": action,
        }
    )
    pm._add_rule(
        {
            "destination": node.ip_address,
            "source_port": 2281,
            "action": action,
        }
    )

    if node.ipv6_address:
        pm._add_rule(
            {
                "source": node.ipv6_address,
                "destination_port": 2281,
                "action": action,
            }
        )
        pm._add_rule(
            {
                "destination": node.ipv6_address,
                "source_port": 2281,
                "action": action,
            }
        )


def restore_secure_zk_connection(pm, node, action="DROP"):
    pm._check_instance(node)
    pm._delete_rule(
        {
            "source": node.ip_address,
            "destination_port": 2281,
            "action": action,
        }
    )
    pm._delete_rule(
        {
            "destination": node.ip_address,
            "source_port": 2281,
            "action": action,
        }
    )

    if node.ipv6_address:
        pm._delete_rule(
            {
                "source": node.ipv6_address,
                "destination_port": 2281,
                "action": action,
            }
        )
        pm._delete_rule(
            {
                "destination": node.ipv6_address,
                "source_port": 2281,
                "action": action,
            }
        )


def check_certificate_switch(first, second):
    # Set first certificate

    change_config_to_key(first)

    # Restart zookeeper the connection

    with PartitionManager() as pm:
        for node in nodes:
            drop_secure_zk_connection(pm, node)
        for node in nodes:
            restore_secure_zk_connection(pm, node)
        clean_logs()

        # Change certificate

        change_config_to_key(second)

        # Time to log

        time.sleep(10)

        # Check information about client certificates reloading in log

        reload_successful = any(check_reload_successful(node, second) for node in nodes)

        # Restart zookeeper to reload the session and clean logs for new check

        for node in nodes:
            drop_secure_zk_connection(pm, node)
        restore_secure_zk_connection(pm, node)
    clean_logs()

    if second == "second":
        try:
            secure_connection_test(started_cluster)
            assert False
        except:
            assert True
    else:
        secure_connection_test(started_cluster)
        error_handshake = any(check_error_handshake(node) == "0\n" for node in nodes)
        assert reload_successful and error_handshake


def test_wrong_cn_cert():
    """Checking the certificate reload with an incorrect CN, the expected behavior is Code: 210."""
    check_certificate_switch("first", "second")


def test_correct_cn_cert():
    """Replacement with a valid certificate, the expected behavior is to restore the connection with Zookeeper."""
    check_certificate_switch("second", "third")
