import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

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
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/test_file.tsv.gz', 'minio', '{minio_secret_key}'
                )
            SELECT * FROM numbers(1000000);
        """
    )

    assert (
        node.query(
            f"""
            SELECT count(*) FROM s3
            (
                'minio://data/test_file.tsv.gz', 'minio', '{minio_secret_key}'
            );
        """
        )
        == "1000000\n"
    )


def test_s3_table_functions_line_as_string(started_cluster):
    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/test_file_line_as_string.tsv.gz', 'minio', '{minio_secret_key}'
                )
            SELECT * FROM numbers(1000000);
        """
    )

    assert (
        node.query(
            f"""
            SELECT _file FROM s3
            (
                'minio://data/*as_string.tsv.gz', 'minio', '{minio_secret_key}', 'LineAsString'
            ) LIMIT 1;
        """
        )
        == node.query(
            f"""
            SELECT _file FROM s3
            (
                'http://minio1:9001/root/data/*as_string.tsv.gz', 'minio', '{minio_secret_key}', 'LineAsString'
            ) LIMIT 1;
        """
        )
    )


def test_s3_question_mark_wildcards(started_cluster):
    """
    Test to verify that S3 URI with question mark wildcards in path works correctly.
    This test verifies the fix for the issue where question marks in s3:// paths were not handled correctly.
    """
    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/wildcard_test_a1.tsv.gz', 'minio', '{minio_secret_key}'
                )
            SELECT 'a1' as id, * FROM numbers(10);
        """
    )
    
    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/wildcard_test_a2.tsv.gz', 'minio', '{minio_secret_key}'
                )
            SELECT 'a2' as id, * FROM numbers(10);
        """
    )
    
    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/wildcard_test_b1.tsv.gz', 'minio', '{minio_secret_key}'
                )
            SELECT 'b1' as id, * FROM numbers(10);
        """
    )
    
    # with question mark wildcard in s3:// URL scheme
    result_s3_scheme = node.query(
        f"""
        SELECT count(*), groupArray(distinct id) as ids FROM s3
        (
            's3://root/data/wildcard_test_??.tsv.gz', 'minio', '{minio_secret_key}'
        );
        """
    )
    
    # Compare with the same query using http:// URL scheme which should also work correctly
    result_http_scheme = node.query(
        f"""
        SELECT count(*), groupArray(distinct id) as ids FROM s3
        (
            'http://minio1:9001/root/data/wildcard_test_??.tsv.gz', 'minio', '{minio_secret_key}'
        );
        """
    )
    
    assert result_s3_scheme == result_http_scheme, "Results differ between s3:// and http:// URL schemes with question mark wildcards"
    
    assert "'a1','a2'" in result_s3_scheme or "'a2','a1'" in result_s3_scheme, "Question mark wildcards didn't match both expected files"
    assert "20" in result_s3_scheme, "Expected 20 rows in total (10 from each matched file)"
