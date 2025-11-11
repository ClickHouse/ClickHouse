import logging
import time
import requests

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


multi_replica_cluster = ClickHouseCluster(__file__)
node1 = multi_replica_cluster.add_instance(
    "node1",
    main_configs=["configs/config_multi.xml"],
    stay_alive=True,
    with_zookeeper=True,
    with_letsencrypt_pebble=True,

    ipv4_address="10.5.11.12",  # never copy-paste this line
)
node2 = multi_replica_cluster.add_instance(
    "node2",
    main_configs=["configs/config_multi.xml"],
    stay_alive=True,
    with_zookeeper=True,

    ipv4_address="10.5.11.13",  # never copy-paste this line
)
node3 = multi_replica_cluster.add_instance(
    "node3",
    main_configs=["configs/config_multi.xml"],
    stay_alive=True,
    with_zookeeper=True,

    ipv4_address="10.5.11.14",  # never copy-paste this line
)

@pytest.fixture(scope="module")
def started_multi_replica_cluster():
    try:
        multi_replica_cluster.start()
        yield multi_replica_cluster
    finally:
        multi_replica_cluster.shutdown()


def test_coordinated_acme_authorization(started_multi_replica_cluster):
    # Let Pebble know where to find our server
    requests.post(
        'http://10.5.11.3:8055/add-a',
        json={'host': 'multi.integration-tests.clickhouse.com', 'addresses': ['10.5.11.12', '10.5.11.13', '10.5.11.14']}
    )

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

            assert "CN=Pebble Intermediate CA" in curl_result
            checked_nodes += 1

        if checked_nodes < 3:
            continue

        zk = started_multi_replica_cluster.get_kazoo_client("zoo1")
        zk.start()

        assert zk.exists("/clickhouse/acme")
        assert zk.exists("/clickhouse/acme/10.5.11.2")
        assert zk.exists("/clickhouse/acme/10.5.11.2/account_private_key")
        assert zk.exists("/clickhouse/acme/10.5.11.2/challenges")
        assert zk.exists("/clickhouse/acme/10.5.11.2/domains")
        assert len(zk.get_children("/clickhouse/acme/10.5.11.2/domains")) == 1

        return

    raise Exception("Failed to get expected certificate issuer")
