import logging

import pytest

from helpers.cluster import ClickHouseCluster

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
                    'minio://data/test_file.tsv.gz', 'minio', 'minio123'
                )
            SELECT * FROM numbers(1000000);
        """
    )

    assert (
        node.query(
            """
            SELECT count(*) FROM s3
            (
                'minio://data/test_file.tsv.gz', 'minio', 'minio123'
            );
        """
        )
        == "1000000\n"
    )


def test_s3_table_functions_line_as_string(started_cluster):
    node.query(
        """
            INSERT INTO FUNCTION s3
                (
                    'minio://data/test_file_line_as_string.tsv.gz', 'minio', 'minio123'
                )
            SELECT * FROM numbers(1000000);
        """
    )

    assert (
        node.query(
            """
            SELECT _file FROM s3
            (
                'minio://data/*as_string.tsv.gz', 'minio', 'minio123', 'LineAsString'
            ) LIMIT 1;
        """
        )
        == node.query(
            """
            SELECT _file FROM s3
            (
                'http://minio1:9001/root/data/*as_string.tsv.gz', 'minio', 'minio123', 'LineAsString'
            ) LIMIT 1;
        """
        )
    )
