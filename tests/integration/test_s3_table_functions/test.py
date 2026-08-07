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
    Test with timeout limit of 1200ms.
    This should raise an Exception and pass.
    """

    with PartitionManager() as pm:
        pm.add_network_delay(node, 1200)

        with pytest.raises(QueryRuntimeException):
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
