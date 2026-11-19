#!/usr/bin/env python3

import os
from multiprocessing.dummy import Pool

import pytest

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
RAFT_SSL_LISTENER_LOG = "Raft ASIO listener initiated on :::9234, SSL enabled"
RAFT_PEER_VERIFICATION_DISABLED_LOG = (
    r"Keeper Raft peer certificate verification is disabled because "
    r"`openSSL\.client\.verificationMode` is set to `none`"
)

cluster = ClickHouseCluster(__file__)
nodes = [
    cluster.add_instance(
        "node1",
        main_configs=[
            "configs/enable_secure_keeper1.xml",
            "configs/ssl_conf.yml",
            "configs/WithoutPassPhrase.crt",
            "configs/WithoutPassPhrase.key",
            "configs/WithPassPhrase.crt",
            "configs/WithPassPhrase.key",
            "configs/rootCA.pem",
            "configs/wrongRootCA.pem",
            "configs/logger.xml",
        ],
        stay_alive=True,
    ),
    cluster.add_instance(
        "node2",
        main_configs=[
            "configs/enable_secure_keeper2.xml",
            "configs/ssl_conf.yml",
            "configs/WithoutPassPhrase.crt",
            "configs/WithoutPassPhrase.key",
            "configs/WithPassPhrase.crt",
            "configs/WithPassPhrase.key",
            "configs/rootCA.pem",
            "configs/wrongRootCA.pem",
            "configs/logger.xml",
        ],
        stay_alive=True,
    ),
    cluster.add_instance(
        "node3",
        main_configs=[
            "configs/enable_secure_keeper3.xml",
            "configs/ssl_conf.yml",
            "configs/WithoutPassPhrase.crt",
            "configs/WithoutPassPhrase.key",
            "configs/WithPassPhrase.crt",
            "configs/WithPassPhrase.key",
            "configs/rootCA.pem",
            "configs/wrongRootCA.pem",
            "configs/logger.xml",
        ],
        stay_alive=True,
    ),
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(nodename, timeout=30.0):
    return ku.get_fake_zk(cluster, nodename, timeout=timeout)


def run_test():
    node_zks = []
    try:
        for node in nodes:
            node_zks.append(get_fake_zk(node.name))

        node_zks[0].create("/test_node", b"somedata1")
        node_zks[1].sync("/test_node")
        node_zks[2].sync("/test_node")

        for node_zk in node_zks:
            assert node_zk.exists("/test_node") is not None
    finally:
        try:
            for zk_conn in node_zks:
                if zk_conn is None:
                    continue
                zk_conn.stop()
                zk_conn.close()
        except:
            pass


def setupSsl(node, filename, password, ssl_conf_file="configs/ssl_conf.yml"):
    if password is None:
        node.copy_file_to_container(
            os.path.join(CURRENT_TEST_DIR, ssl_conf_file),
            "/etc/clickhouse-server/config.d/ssl_conf.yml",
        )

        node.replace_in_config(
            "/etc/clickhouse-server/config.d/ssl_conf.yml",
            "WithoutPassPhrase",
            filename,
        )
        return

    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs/ssl_conf_password.yml"),
        "/etc/clickhouse-server/config.d/ssl_conf.yml",
    )

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/ssl_conf.yml",
        "WithoutPassPhrase",
        filename,
    )

    node.replace_in_config(
        "/etc/clickhouse-server/config.d/ssl_conf.yml",
        "PASSWORD",
        password,
    )


def stop_all_clickhouse():
    for node in nodes:
        node.stop_clickhouse()

    for node in nodes:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "set -e; "
                "for log in "
                "/var/log/clickhouse-server/clickhouse-server.log "
                "/var/log/clickhouse-server/clickhouse-server.err.log "
                "/var/log/clickhouse-server/clickhouse-server.log.wait_for_log_line; "
                "do "
                "if [ -e \"$log\" ]; then truncate -s 0 \"$log\"; fi; "
                "done",
            ]
        )
        node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination"])


def start_clickhouse(node):
    node.start_clickhouse()


def start_all_clickhouse_processes():
    p = Pool(3)
    waiters = []

    for node in nodes:
        waiters.append(p.apply_async(start_clickhouse, args=(node,)))

    for waiter in waiters:
        waiter.wait()


def wait_all_clickhouse_connected():
    for node in nodes:
        ku.wait_until_connected(cluster, node)


def start_all_clickhouse():
    start_all_clickhouse_processes()
    wait_all_clickhouse_connected()


def get_log_anchors():
    return {node.name: max(1, node.count_log_lines()) for node in nodes}


def wait_for_raft_ssl_listener(node, log_anchor):
    node.wait_for_log_line(
        RAFT_SSL_LISTENER_LOG, look_behind_lines=f"+{log_anchor}"
    )


def check_valid_configuration(
    filename, password, ssl_conf_file="configs/ssl_conf.yml", expected_log_line=None
):
    stop_all_clickhouse()
    for node in nodes:
        setupSsl(node, filename, password, ssl_conf_file)

    log_anchors = get_log_anchors()
    start_all_clickhouse()
    wait_for_raft_ssl_listener(nodes[0], log_anchors[nodes[0].name])

    if expected_log_line is not None:
        nodes[0].wait_for_log_line(
            expected_log_line, look_behind_lines=f"+{log_anchors[nodes[0].name]}"
        )

    run_test()


def check_invalid_configuration(filename, password):
    stop_all_clickhouse()
    for node in nodes:
        setupSsl(node, filename, password)

    log_anchors = get_log_anchors()
    nodes[0].start_clickhouse()
    wait_for_raft_ssl_listener(nodes[0], log_anchors[nodes[0].name])
    nodes[0].wait_for_log_line("failed to connect to peer.*Connection refused")


def check_peer_verification_failure(ssl_conf_file):
    stop_all_clickhouse()
    for node in nodes:
        setupSsl(node, "WithoutPassPhrase", None, ssl_conf_file)

    log_anchors = get_log_anchors()
    start_all_clickhouse_processes()
    wait_for_raft_ssl_listener(nodes[0], log_anchors[nodes[0].name])
    nodes[0].wait_for_log_line(
        "failed SSL handshake with peer.*certificate verify failed",
        look_behind_lines=f"+{log_anchors[nodes[0].name]}",
    )

    with pytest.raises(Exception, match="timeout.*serving requests"):
        ku.wait_until_connected(cluster, nodes[0], timeout=5)


def test_secure_raft_works(started_cluster):
    check_valid_configuration("WithoutPassPhrase", None)


def test_secure_raft_client_verification_mode_none_skips_bad_ca(started_cluster):
    check_valid_configuration(
        "WithoutPassPhrase",
        None,
        ssl_conf_file="configs/ssl_conf_client_verification_none_wrong_ca.yml",
        expected_log_line=RAFT_PEER_VERIFICATION_DISABLED_LOG,
    )


def test_secure_raft_client_verification_mode_relaxed_rejects_bad_ca(started_cluster):
    check_peer_verification_failure(
        ssl_conf_file="configs/ssl_conf_client_verification_relaxed_wrong_ca.yml",
    )


def test_secure_raft_works_with_password(started_cluster):
    check_valid_configuration("WithoutPassPhrase", "unusedpassword")
    check_invalid_configuration("WithPassPhrase", "wrongpassword")
    check_invalid_configuration("WithPassPhrase", "")
    check_valid_configuration("WithPassPhrase", "test")
    check_invalid_configuration("WithPassPhrase", None)


def test_secure_raft_works_with_cipher_list(started_cluster):
    check_valid_configuration(
        "WithoutPassPhrase", None, ssl_conf_file="configs/ssl_conf_cipher.yml"
    )
