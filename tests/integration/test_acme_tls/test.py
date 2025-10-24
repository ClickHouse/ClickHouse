import logging
import os
import time
import json

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

single_replica_cluster = ClickHouseCluster(__file__)
node = single_replica_cluster.add_instance(
    "node_acme",
    main_configs=["configs/config.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

multi_replica_cluster = ClickHouseCluster(__file__)
node1 = multi_replica_cluster.add_instance(
    "node1",
    main_configs=["configs/config_multi.xml"],
    stay_alive=True,
    with_zookeeper=True,
)
node2 = multi_replica_cluster.add_instance(
    "node2",
    main_configs=["configs/config_multi.xml"],
    stay_alive=True,
    with_zookeeper=True,
)
node3 = multi_replica_cluster.add_instance(
    "node3",
    main_configs=["configs/config_multi.xml"],
    stay_alive=True,
    with_zookeeper=True,
)


def start_metadata_server(cluster, node_name):
    script_dir = os.path.join(os.path.dirname(__file__), "mocks")
    start_mock_servers(
        cluster,
        script_dir,
        [
            (
                "acme_server.py",
                node_name,
                "80",
            )
        ],
    )


@pytest.fixture(scope="module")
def started_single_replica_cluster():
    try:
        single_replica_cluster.start()
        start_metadata_server(single_replica_cluster, "node_acme")
        yield single_replica_cluster
    finally:
        single_replica_cluster.shutdown()


@pytest.fixture(scope="module")
def started_multi_replica_cluster():
    try:
        multi_replica_cluster.start()
        start_metadata_server(multi_replica_cluster, "node1")
        yield multi_replica_cluster
    finally:
        multi_replica_cluster.shutdown()


def test_acme_authorization(started_single_replica_cluster):
    for _ in range(60):
        time.sleep(1)

        curl_result = node.exec_in_container(
            [
                "bash",
                "-c",
                "curl -k -v 'https://127.0.0.1:3443' 2>&1 | grep issuer: || true",
            ]
        )

        if not curl_result:
            continue

        assert "O=ClickHouse; CN=Integration tests" in curl_result

        curl_result = node.exec_in_container(["curl", "http://node_acme/counters"])
        counters = json.loads(curl_result)
        print(counters)

        assert counters["nonce_count"] > 0
        assert counters["order_count"] == 1
        assert counters["csr_count"] == 1
        assert counters["jwk_count"] == 1
        assert counters["call_counters"]["new_account"] == 1
        assert counters["call_counters"]["new_order"] == 1
        assert counters["call_counters"]["process_challenge"] == 1
        assert counters["call_counters"]["finalize_order"] == 1

        zk = started_single_replica_cluster.get_kazoo_client("zoo1")
        zk.start()

        assert zk.exists("/clickhouse/acme")
        assert zk.exists("/clickhouse/acme/node_acme")
        assert zk.exists("/clickhouse/acme/node_acme/account_private_key")
        assert zk.exists("/clickhouse/acme/node_acme/challenges")
        assert len(zk.get_children("/clickhouse/acme/node_acme/challenges")) == 1
        assert zk.exists("/clickhouse/acme/node_acme/domains")
        assert len(zk.get_children("/clickhouse/acme/node_acme/domains")) == 1

        return

    raise Exception("Failed to get expected certificate issuer")


def test_coordinated_acme_authorization(started_multi_replica_cluster):
    for _ in range(60):
        time.sleep(1)

        checked_nodes = 0
        for node_to_check in [node1, node2, node3]:
            curl_result = node_to_check.exec_in_container(
                [
                    "bash",
                    "-c",
                    "curl -k -v 'https://127.0.0.1:3443' 2>&1 | grep issuer: || true",
                ]
            )

            if not curl_result:
                continue

            assert "O=ClickHouse; CN=Integration tests" in curl_result
            checked_nodes += 1

        if checked_nodes < 3:
            continue

        curl_result = node1.exec_in_container(["curl", "http://node1/counters"])
        counters = json.loads(curl_result)

        assert counters["nonce_count"] > 0
        assert counters["order_count"] == 1
        assert counters["csr_count"] == 1
        assert counters["jwk_count"] == 1
        assert counters["call_counters"]["new_account"] == 3
        assert counters["call_counters"]["new_order"] == 1
        assert counters["call_counters"]["process_challenge"] == 1
        assert counters["call_counters"]["finalize_order"] == 1

        return

    raise Exception("Failed to get expected certificate issuer")
