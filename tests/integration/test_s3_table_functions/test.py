import logging
import os

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


def upload_lance_dataset_to_minio(started_cluster, remote_prefix):
    local_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../queries/0_stateless/data_lance/basic.lance",
        )
    )
    for root, _, files in os.walk(local_path):
        for filename in files:
            local_file = os.path.join(root, filename)
            relative_path = os.path.relpath(local_file, local_path)
            remote_path = os.path.join(remote_prefix, relative_path)
            started_cluster.minio_client.fput_object(
                bucket_name=started_cluster.minio_bucket,
                object_name=remote_path,
                file_path=local_file,
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


def test_lance_s3_table_function(started_cluster):
    if node.query("SELECT count() FROM system.table_functions WHERE name = 'lanceS3'") == "0\n":
        pytest.skip("lanceS3 table function is not available in this build")

    remote_prefix = "data/lance/basic.lance"
    upload_lance_dataset_to_minio(started_cluster, remote_prefix)

    assert (
        node.query(
            """
            SELECT id, name, score
            FROM lanceS3(nc_s3, filename = 'lance/basic.lance')
            ORDER BY id
            """
        )
        == "1\ta\t10\n2\tb\t\\N\n3\tc\t30\n"
    )
