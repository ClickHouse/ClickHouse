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
    
    result_s3_scheme = node.query(f"""
        SELECT count() AS c, arraySort(groupArray(DISTINCT id)) AS ids
        FROM s3('s3://data/wildcard_test_??.tsv.gz', 'minio', '{minio_secret_key}')
        FORMAT TSV
    """)

    result_http_scheme = node.query(f"""
        SELECT count() AS c, arraySort(groupArray(DISTINCT id)) AS ids
        FROM s3('http://minio1:9001/data/wildcard_test_??.tsv.gz', 'minio', '{minio_secret_key}')
        FORMAT TSV
    """)

    assert result_s3_scheme == result_http_scheme
    assert result_s3_scheme.startswith('20\t')
    assert "['a1','a2']" in result_s3_scheme or "['a2','a1']" in result_s3_scheme

def test_s3_presigned_url_works_with_s3_engine(started_cluster):
    """
    Regression test for pre-signed URLs:
    - Previously, the S3 table function misinterpreted '?' in '?X-Amz-...' as a wildcard
      or encoded it in a way that broke the signature, while the URL engine worked.
    - This test ensures the S3 table function correctly preserves the query string and can read the object.
    """
    import io
    import pytest
    from datetime import timedelta

    minio_mod = pytest.importorskip("minio")
    from minio import Minio

    client = Minio("minio1:9001", access_key="minio", secret_key=minio_secret_key, secure=False)
    bucket = "data"
    object_name = "presigned_url_test/presigned_test.csv"

    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    except Exception:
        # Bucket may already exist; ignore race
        pass

    # Upload a tiny CSV (3 rows, 1 column)
    payload = b"row1\nrow2\nrow3\n"
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(payload),
        length=len(payload),
        content_type="text/csv",
    )

    # Generate a pre-signed GET URL with ?X-Amz-* query parameters
    url = client.presigned_get_object(bucket, object_name, expires=timedelta(minutes=5))

    cnt_url = node.query(f"SELECT count() FROM url('{url}', 'CSV', 'x String')")
    assert cnt_url.strip() == "3", f"URL engine returned unexpected row count: {cnt_url!r}"

    cnt_s3 = node.query(f"SELECT count() FROM s3('{url}', 'CSV', 'x String')")
    assert cnt_s3.strip() == "3", f"S3 table function returned unexpected row count: {cnt_s3!r}"

    assert cnt_s3 == cnt_url, "S3 and URL engines disagree on row count for the same pre-signed URL"
