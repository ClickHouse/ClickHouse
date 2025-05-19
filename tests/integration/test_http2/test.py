from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

import pytest


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/http2.xml",
        "configs/server.crt",
        "configs/server.key",
    ],
)


CRT_PATH = "/etc/clickhouse-server/config.d/server.crt"
QUERY = "SELECT number FROM numbers(5)"
EXPECTED_RESULT = "0\n1\n2\n3\n4\n"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_prior_knowledge(started_cluster):
    res = node.exec_in_container(
        [
            "curl",
            "--http2-prior-knowledge",
            "--cacert", CRT_PATH,
            "-d", QUERY,
            "http://localhost:8123/",
        ]
    )
    assert res == EXPECTED_RESULT


def test_alpn(started_cluster):
    res = node.exec_in_container(
        [
            "bash",
            "-c",
            f"curl --verbose --http2 --cacert '{CRT_PATH}' -d '{QUERY}' 'https://localhost:8443/' 2>&1",
        ]
    )
    assert "ALPN, server accepted to use h2" in res
    assert res.endswith(EXPECTED_RESULT)
