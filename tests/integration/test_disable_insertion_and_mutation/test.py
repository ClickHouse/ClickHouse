import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)

writing_node = cluster.add_instance(
    "writing_node",
    main_configs=["config/writing_node.xml", "config/cluster.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
reading_node = cluster.add_instance(
    "reading_node",
    main_configs=[
        "config/reading_node.xml",
        "config/cluster.xml",
        "config/minio_scheme_mapper.xml",
    ],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_disable_insertion_and_mutation_with_temp_table(started_cluster):
    # Allow insert into temp table in reading node
    # Use the same session_id because the temporary table only exist in one session
    session_id = str(uuid.uuid4())
    reading_node.http_query(
        """CREATE TEMPORARY TABLE my_tmp_table (key UInt64, value String) ENGINE=Memory""",
        params={"session_id": session_id},
    )

    reading_node.http_query(
        """INSERT INTO my_tmp_table VALUES (1, 'hello'), (2, 'world')""",
        params={"session_id": session_id},
    )


def test_disable_insertion_and_mutation(started_cluster):
    writing_node.query("DROP TABLE IF EXISTS my_table ON CLUSTER default SYNC")
    writing_node.query(
        """CREATE TABLE my_table on cluster default (key UInt64, value String) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/default.my_table', '{replica}') ORDER BY key partition by (key % 5) """
    )

    assert "QUERY_IS_PROHIBITED" in reading_node.query_and_get_error(
        "INSERT INTO my_table VALUES (1, 'hello')"
    )

    assert "QUERY_IS_PROHIBITED" in reading_node.query_and_get_error(
        "INSERT INTO my_table SETTINGS async_insert = 1 VALUES (1, 'hello')"
    )

    assert "QUERY_IS_PROHIBITED" in reading_node.query_and_get_error(
        "ALTER TABLE my_table delete where 1"
    )

    assert "QUERY_IS_PROHIBITED" in reading_node.query_and_get_error(
        "ALTER table my_table update key = 1 where 1"
    )

    assert "QUERY_IS_PROHIBITED" in reading_node.query_and_get_error(
        "ALTER TABLE my_table drop partition 0"
    )

    reading_node.query("SELECT * from my_table")
    writing_node.query("INSERT INTO my_table VALUES (1, 'hello')")
    writing_node.query("ALTER TABLE my_table delete where 1")
    writing_node.query("ALTER table my_table update value = 'no hello' where 1")

    # Wait for all replicas to apply the metadata change (alter_sync = 2) so that
    # the following reads on writing_node do not race the metadata replication.
    reading_node.query(
        "ALTER TABLE my_table ADD COLUMN new_column UInt64 SETTINGS alter_sync = 2"
    )
    writing_node.query("SELECT new_column from my_table")
    reading_node.query("SELECT new_column from my_table")

    reading_node.query(
        "ALter Table my_table MODIFY COLUMN new_column String SETTINGS alter_sync = 2"
    )

    assert "new_column\tString" in reading_node.query("DESC my_table")

    assert "new_column\tString" in writing_node.query("DESC my_table")
    
    writing_node.query("DROP TABLE IF EXISTS my_table ON CLUSTER default SYNC")


def test_disable_insertion_and_mutation_with_external_tables(started_cluster):
    """Test that inserts into external tables (S3, Kafka, MySQL, PostgreSQL) are allowed
    even when disable_insertion_and_mutation is set to true"""

    # Test S3 table insert - should be allowed
    # Use unique path to avoid conflicts between test runs
    unique_id = str(uuid.uuid4())
    reading_node.query("DROP TABLE IF EXISTS s3_table")
    reading_node.query(
        f"""
        CREATE TABLE s3_table (key UInt64, value String)
        ENGINE=S3('http://minio1:9001/root/data/test_external_{unique_id}.csv', 'minio', '{minio_secret_key}', 'CSV')
        """
    )

    # This should succeed even on reading_node with disable_insertion_and_mutation=true
    reading_node.query("INSERT INTO s3_table VALUES (1, 'hello'), (2, 'world')")

    # Verify data was written
    result = reading_node.query("SELECT count() FROM s3_table")
    assert "2" in result

    reading_node.query("DROP TABLE IF EXISTS s3_table")


def test_disable_insertion_and_mutation_deferred_url_named_collection(started_cluster):
    """A URL(named_collection) table attached while its collection was missing is served by a lazy
    proxy. The proxy must report the delegate's engine classification, so an object-storage-backed
    table gets the same external-engine exemption as the eager `ENGINE = URL(...)` form."""

    unique_id = uuid.uuid4().hex
    collection = f"nc_deferred_{unique_id}"
    # config/minio_scheme_mapper.xml maps `s3://<bucket>/<key>` to the test MinIO and supplies the
    # endpoint credentials, so this dispatches to the object storage engine. The URL engine's named
    # collection schema accepts no credential keys, hence the server-side endpoint config.
    create_collection = f"""
        CREATE NAMED COLLECTION {collection} AS
            url = 's3://data/test_deferred_{unique_id}.csv', format = 'CSV'
        """
    ignore_deps = {"check_named_collection_dependencies": 0}

    reading_node.query("DROP TABLE IF EXISTS deferred_url_table")
    reading_node.query(
        f"DROP NAMED COLLECTION IF EXISTS {collection}", settings=ignore_deps
    )

    reading_node.query(create_collection)
    reading_node.query(
        f"CREATE TABLE deferred_url_table (key UInt64, value String) ENGINE = URL({collection})"
    )

    # Drive the table onto the deferred path: ATTACH with the collection gone is what server startup
    # replays from metadata, so the lazy proxy is what answers queries afterwards.
    reading_node.query("DETACH TABLE deferred_url_table")
    reading_node.query(f"DROP NAMED COLLECTION {collection}", settings=ignore_deps)
    reading_node.query("ATTACH TABLE deferred_url_table")
    reading_node.query(create_collection)

    # Allowed despite disable_insertion_and_mutation=true: the delegate is object storage. Without
    # the classification forwarding this throws QUERY_IS_PROHIBITED, while the eager
    # `ENGINE = URL('s3://...')` form inserts.
    reading_node.query("INSERT INTO deferred_url_table VALUES (1, 'hello'), (2, 'world')")
    assert reading_node.query("SELECT count() FROM deferred_url_table").strip() == "2"

    reading_node.query("DROP TABLE IF EXISTS deferred_url_table")
    reading_node.query(
        f"DROP NAMED COLLECTION IF EXISTS {collection}", settings=ignore_deps
    )
