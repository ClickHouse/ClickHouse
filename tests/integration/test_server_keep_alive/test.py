import logging
import random

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/keep_alive_settings.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_max_keep_alive_requests_on_user_side(start_cluster):
    # In this test we have `keep_alive_timeout` set to one hour to never trigger connection reset by timeout, `max_keep_alive_requests` is set to 5.
    # We expect server to close connection after each 5 requests. We detect connection reset by change in src port.
    # So the first 5 requests should come from the same port, the following 5 requests should come from another port.

    log_comments = []
    for _ in range(10):
        rand_id = random.randint(0, 1000000)
        log_comment = f"test_requests_with_keep_alive_{rand_id}"
        log_comments.append(log_comment)
    log_comments = sorted(log_comments)

    session = requests.Session()
    for i in range(10):
        session.get(
            f"http://{node.ip_address}:8123/?query=select%201&log_comment={log_comments[i]}"
        )

    ports = node.query(
        f"""
        SYSTEM FLUSH LOGS;

        SELECT port
          FROM system.query_log
         WHERE log_comment IN ({", ".join(f"'{comment}'" for comment in log_comments)}) AND type = 'QueryFinish'
      ORDER BY log_comment
        """
    ).split("\n")[:-1]

    expected = 5 * [ports[0]] + [ports[5]] * 5

    assert ports == expected
