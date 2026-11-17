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
    # `test_url_s3_scheme_with_parallel_replicas` needs a `ReplicatedMergeTree` destination so that the
    # distributed `INSERT ... SELECT` path is actually entered.
    with_zookeeper=True,
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

    # The destination must support replication: `distributedWriteIntoReplicatedMergeTreeOrDataLakeFromClusterStorage`
    # returns early for a plain `MergeTree`, so with one the `INSERT` would never even reach the point where the
    # source is examined and the regression could not show up. With a `ReplicatedMergeTree` the distributed path is
    # entered and declines the query only because the delegated `url` resolved to a plain storage rather than to an
    # `IStorageCluster` - which is exactly the property under test. If the delegate ever fans out again, the
    # forwarded query names `urlCluster('s3://...')`, which the check of `system.query_log` below catches.
    #
    # A row count alone cannot tell "declined because of the source" from "declined earlier for another reason",
    # so the same `INSERT` from `s3('s3://...')` serves as a positive control: with the identical destination and
    # settings it must be forwarded to the cluster. The two queries differ only in the source, so the `url` one
    # reached the source check and was declined there.
    # Separate tables, so that the deduplication of `ReplicatedMergeTree` cannot swallow the second insert.
    run_id = uuid.uuid4().hex
    query_ids = {}
    for function in ("url", "s3"):
        table = f"{function}_s3_parallel_replicas"
        query_ids[function] = f"{table}_{run_id}"
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node.query(
            f"""
                CREATE TABLE {table} (a UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}_{run_id}', 'r1') ORDER BY a
            """
        )
        node.query(
            f"""
                INSERT INTO {table}
                SELECT * FROM {function}('s3://data/parallel_replicas_url.csv', 'CSV', 'a UInt32')
                {parallel_replicas_settings}, parallel_distributed_insert_select = 2, log_queries = 1
            """,
            query_id=query_ids[function],
        )

        # The rows must be inserted exactly once, not once per replica.
        assert node.query(f"SELECT count() FROM {table}") == "10\n"

    node.query("SYSTEM FLUSH LOGS query_log")

    def forwarded_inserts(query_id):
        return node.query(
            f"""
                SELECT count(), countIf(query ILIKE '%Cluster(%'), sum(read_rows)
                FROM system.query_log
                WHERE initial_query_id = '{query_id}'
                    AND is_initial_query = 0
                    AND query_kind = 'Insert'
                    AND type = 'QueryFinish'
                    AND event_date >= yesterday()
            """
        )

    # Control: the forwarded `INSERT` naming `s3Cluster` ran and read the file once. All three replicas of the
    # `parallel_replicas` cluster are the same `node:9000`, and `Cluster::getClusterWithReplicasAsShards` skips
    # duplicate hosts, so the cluster storage sees a single shard and the query is forwarded exactly once.
    assert forwarded_inserts(query_ids["s3"]) == "1\t1\t10\n"
    # The delegated `url` was not forwarded at all: it was inserted locally by the initiator.
    assert forwarded_inserts(query_ids["url"]) == "0\t0\t0\n"

    for function in ("url", "s3"):
        node.query(f"DROP TABLE {function}_s3_parallel_replicas SYNC")
