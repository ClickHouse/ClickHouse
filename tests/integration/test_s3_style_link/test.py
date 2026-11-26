import logging
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/minio.xml",
        "configs/config.d/parallel_replicas.xml",
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
                ) SETTINGS s3_truncate_on_insert=1
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
                ) SETTINGS s3_truncate_on_insert=1
            SELECT * FROM numbers(1000000);
        """
    )

    bucket = started_cluster.minio_bucket
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
                'http://minio1:9001/{bucket}/data/*as_string.tsv.gz', 'minio', '{minio_secret_key}', 'LineAsString'
            ) LIMIT 1;
        """
        )
    )


def test_s3_question_mark_wildcards(started_cluster):
    # Create sample files under the default bucket (root) with folder 'data/'
    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/wildcard_test_a1.tsv.gz', 'minio', '{minio_secret_key}'
                ) SETTINGS s3_truncate_on_insert=1
            SELECT 'a1' as id, * FROM numbers(10);
        """
    )

    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/wildcard_test_a2.tsv.gz', 'minio', '{minio_secret_key}'
                ) SETTINGS s3_truncate_on_insert=1
            SELECT 'a2' as id, * FROM numbers(10);
        """
    )

    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/wildcard_test_b1.tsv.gz', 'minio', '{minio_secret_key}'
                ) SETTINGS s3_truncate_on_insert=1
            SELECT 'b1' as id, * FROM numbers(10);
        """
    )

    result_s3_scheme = node.query(f"""
        SELECT count() AS c, arraySort(groupArray(DISTINCT id)) AS ids
        FROM s3('s3://data/wildcard_test_a?.tsv.gz', 'minio', '{minio_secret_key}', 'TSV', 'id String, number UInt64')
        FORMAT TSV
    """)

    bucket = started_cluster.minio_bucket
    result_http_scheme = node.query(f"""
        SELECT count() AS c, arraySort(groupArray(DISTINCT id)) AS ids
        FROM s3('http://minio1:9001/{bucket}/data/wildcard_test_a?.tsv.gz', 'minio', '{minio_secret_key}', 'TSV', 'id String, number UInt64')
        FORMAT TSV
    """)

    assert result_s3_scheme == result_http_scheme
    assert result_s3_scheme.startswith('20\t')
    assert "['a1','a2']" in result_s3_scheme or "['a2','a1']" in result_s3_scheme


def test_url_s3_scheme_with_parallel_replicas(started_cluster):
    """
    `url('s3://...')` is delegated to the `s3` backend, but the query text still names `url`.
    The cluster fan-out of `parallel_replicas_for_cluster_engines` rewrites the forwarded query
    from that surface name, so it used to send `urlCluster('s3://...')` - a shape `urlCluster`
    rejects - both for a plain `SELECT` and for the distributed `INSERT ... SELECT`.
    """
    node.query(
        f"""
            INSERT INTO FUNCTION s3
                (
                    'minio://data/parallel_replicas_url.csv', 'minio', '{minio_secret_key}',
                    'CSV', 'a UInt32'
                ) SETTINGS s3_truncate_on_insert=1
            SELECT number FROM numbers(10);
        """
    )

    parallel_replicas_settings = """
        SETTINGS cluster_for_parallel_replicas = 'parallel_replicas',
                 enable_parallel_replicas = 1,
                 max_parallel_replicas = 3,
                 parallel_replicas_for_cluster_engines = 1
    """

    assert (
        node.query(
            f"""
            SELECT count() FROM url('s3://data/parallel_replicas_url.csv', 'CSV', 'a UInt32')
            {parallel_replicas_settings}
        """
        )
        == "10\n"
    )

    node.query("DROP TABLE IF EXISTS url_s3_parallel_replicas SYNC")
    node.query(
        "CREATE TABLE url_s3_parallel_replicas (a UInt32) ENGINE = MergeTree ORDER BY a"
    )
    node.query(
        f"""
            INSERT INTO url_s3_parallel_replicas
            SELECT * FROM url('s3://data/parallel_replicas_url.csv', 'CSV', 'a UInt32')
            {parallel_replicas_settings}, parallel_distributed_insert_select = 2
        """
    )

    # The rows must be inserted exactly once, not once per replica.
    assert node.query("SELECT count() FROM url_s3_parallel_replicas") == "10\n"
    node.query("DROP TABLE url_s3_parallel_replicas SYNC")


def test_partitioned_write_filtered_grant_with_scheme_mapper(started_cluster):
    """
    The server config of this module, `configs/config.d/minio.xml`, maps the `minio` scheme with `url_scheme_mappers`:

        <minio><to>http://minio1:9001/root/{bucket}</to></minio>

    `S3::URI` puts the host of a `minio://` URI in place of `{bucket}`, so `minio://data/x.tsv` becomes
    `http://minio1:9001/root/data/x.tsv`: the key `data/x.tsv` in the bucket `root`. The key then has a prefix, `data/`,
    that the path of the URI as the user typed it does not have. Each partition of a partitioned write must still be
    checked against a filtered grant on the typed URI.
    """

    suffix = uuid.uuid4().hex
    user = f"partition_writer_{suffix}"
    allowed = f"allowed_{suffix}"
    protected = f"protected_{suffix}"
    columns = "column1 UInt32, column2 UInt32, column3 UInt32"
    template = f"minio://data/{allowed}/{{_partition_id}}.tsv"

    def insert_error(partition_by):
        """Insert the row into `template` as the user; return the error text, empty on success."""
        _, error = node.query_and_get_answer_with_error(
            f"INSERT INTO FUNCTION s3('{template}', 'minio', '{minio_secret_key}', 'TSV', '{columns}') "
            f"PARTITION BY {partition_by} VALUES (1, 2, 3)",
            user=user,
        )
        return error

    def keys(prefix):
        """List the keys under `prefix` in the bucket `root`."""
        return [obj.object_name for obj in cluster.minio_client.list_objects(cluster.minio_bucket, prefix=prefix, recursive=True)]

    node.query(f"CREATE USER {user}")
    node.query(f"GRANT CREATE TEMPORARY TABLE ON *.* TO {user}")
    node.query(f"GRANT WRITE ON S3('^minio://data/{allowed}/.*$') TO {user}")

    try:
        # Happy path: a partition inside the granted prefix is written to the mapped key.
        assert insert_error("toString(column1)") == ""
        assert keys(f"data/{allowed}/") == [f"data/{allowed}/1.tsv"]

        # Reject: a partition id that leaves the granted prefix is refused, and nothing is written there.
        assert "Not enough privileges" in insert_error(f"concat('../{protected}/owned_', toString(column1))")
        assert keys(f"data/{protected}/") == []
    finally:
        node.query(f"DROP USER IF EXISTS {user}")
