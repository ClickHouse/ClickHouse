import logging

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/minio.xml",
    ],
    user_configs=[
        "configs/users.d/users.xml",
    ],
    with_minio=True,
)

settings = {
    "s3_max_connections": "1",
    "max_insert_threads": "1",
    "s3_truncate_on_insert": "1",
    "s3_min_upload_part_size": "33554432",
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        logging.info("Stopping cluster")
        cluster.shutdown()
        logging.info("Cluster stopped")


def test_s3_table_functions(started_cluster):
    """
    Simple test to check s3 table function functionalities
    """
    node.query(
        """
            INSERT INTO FUNCTION s3
                (
                    nc_s3,
                    filename = 'test_file.tsv.gz',
                    format = 'TSV',
                    structure = 'number UInt64',
                    compression_method = 'gz'
                )
            SELECT * FROM numbers(1000000)
        """,
        settings=settings,
    )

    assert (
        node.query(
            """
            SELECT count(*) FROM s3
            (
                nc_s3,
                filename = 'test_file.tsv.gz',
                format = 'TSV',
                structure = 'number UInt64',
                compression_method = 'gz'
            );
        """
        )
        == "1000000\n"
    )


def test_s3_table_functions_timeouts(started_cluster):
    """
    A 1200ms network delay must make the S3 write time out and raise.
    """

    # Make the S3 request timeout (not the connect timeout) the single failure mechanism:
    # disable adaptive timeouts and keep the connect timeout above the delay, so the write
    # can only fail via s3_request_timeout_ms. This exercises the send/receive idleness
    # timeout that applies to every attempt on both fresh and reused (pooled keep-alive)
    # connections, which is the path that was silently not timing out before.
    timeout_settings = {
        **settings,
        "s3_use_adaptive_timeouts": "0",
        "s3_connect_timeout_ms": "10000",
        "s3_request_timeout_ms": "500",
    }

    with PartitionManager() as pm:
        pm.add_network_delay(node, 1200)

        with pytest.raises(QueryRuntimeException, match="Timeout"):
            node.query(
                """
                INSERT INTO FUNCTION s3
                    (
                        nc_s3,
                        filename = 'test_file.tsv.gz',
                        format = 'TSV',
                        structure = 'number UInt64',
                        compression_method = 'gz'
                    )
                SELECT * FROM numbers(1000000)
            """,
                settings=timeout_settings,
            )
