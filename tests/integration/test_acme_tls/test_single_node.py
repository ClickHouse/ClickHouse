import logging
import time
import requests

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

single_replica_cluster = ClickHouseCluster(__file__)
node = single_replica_cluster.add_instance(
    "node_acme",
    main_configs=["configs/config.xml"],
    stay_alive=True,
    with_zookeeper=True,
    with_letsencrypt_pebble=True,

    ipv4_address="10.5.11.11",  # never copy-paste this line
)


@pytest.fixture(scope="module")
def started_single_replica_cluster():
    try:
        single_replica_cluster.start()
        yield single_replica_cluster
    finally:
        single_replica_cluster.shutdown()


def test_acme_authorization(started_single_replica_cluster):
    # Let Pebble know where to find our server
    requests.post(
        'http://10.5.11.3:8055/add-a',
        json={'host': 'single.integration-tests.clickhouse.com', 'addresses': ['10.5.11.11']}
    )

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

        assert "CN=Pebble Intermediate CA" in curl_result

        # curl_result = node.exec_in_container(["curl", "http://node_acme/counters"])
        # counters = json.loads(curl_result)
        # print(counters)
        #
        # assert counters["nonce_count"] > 0
        # assert counters["order_count"] == 1
        # assert counters["csr_count"] == 1
        # assert counters["jwk_count"] == 1
        # assert counters["call_counters"]["new_account"] == 1
        # assert counters["call_counters"]["new_order"] == 1
        # assert counters["call_counters"]["process_challenge"] == 1
        # assert counters["call_counters"]["finalize_order"] == 1
        #
        # zk = started_single_replica_cluster.get_kazoo_client("zoo1")
        # zk.start()
        #
        # assert zk.exists("/clickhouse/acme")
        # assert zk.exists("/clickhouse/acme/node_acme")
        # assert zk.exists("/clickhouse/acme/node_acme/account_private_key")
        # assert zk.exists("/clickhouse/acme/node_acme/challenges")
        # assert len(zk.get_children("/clickhouse/acme/node_acme/challenges")) == 1
        # assert zk.exists("/clickhouse/acme/node_acme/domains")
        # assert len(zk.get_children("/clickhouse/acme/node_acme/domains")) == 1

        return

    raise Exception("Failed to get expected certificate issuer")
