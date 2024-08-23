import logging
import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/cluster.xml",
            ],
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        node = cluster.instances["node"]
        node.query(
            """
            CREATE TABLE tab
            (
                a DateTime,
                pk String
            ) Engine = MergeTree() ORDER BY pk;
            """
        )

        yield cluster
    finally:
        cluster.shutdown()


def test_incorrect_datetime_format(cluster):
    """
    Test for an MSan issue which is caused by parsing incorrect datetime string
    """

    node = cluster.instances["node"]

    res = node.query("SELECT count(*) FROM tab WHERE a = '2024-08-06 09:58:09'").strip()
    assert res == "0"

    error = node.query_and_get_error(
        "SELECT count(*) FROM tab WHERE a = '2024-08-06 09:58:0'"
    ).strip()
    assert "Cannot parse time component of DateTime 09:58:0" in error

    error = node.query_and_get_error(
        "SELECT count(*) FROM tab WHERE a = '2024-08-0 09:58:09'"
    ).strip()
    assert "Cannot convert string '2024-08-0 09:58:09' to type DateTime" in error
