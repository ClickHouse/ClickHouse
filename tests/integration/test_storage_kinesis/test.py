import json
import logging
import time

import boto3
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/kinesis.xml"],
    with_localstack=True,
    stay_alive=True,
)

LOCALSTACK_ENDPOINT = "http://localstack:4566"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"


def get_kinesis_client():
    return boto3.client(
        "kinesis",
        endpoint_url=f"http://{cluster.localstack_host}:{cluster.localstack_port}",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def create_stream(client, stream_name, shard_count=1):
    client.create_stream(StreamName=stream_name, ShardCount=shard_count)
    waiter = client.get_waiter("stream_exists")
    waiter.wait(StreamName=stream_name)


def put_records(client, stream_name, records):
    """Put a list of string records into a Kinesis stream."""
    entries = [
        {"Data": r.encode(), "PartitionKey": str(i)}
        for i, r in enumerate(records)
    ]
    client.put_records(Records=entries, StreamName=stream_name)


def delete_stream(client, stream_name):
    try:
        client.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
    except Exception:
        pass


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_kinesis_table(instance, table_name, stream_name, columns, fmt="JSONEachRow", extra_settings=""):
    instance.query(
        f"""
        CREATE TABLE {table_name} ({columns})
        ENGINE = Kinesis
        SETTINGS
            kinesis_stream_name = '{stream_name}',
            kinesis_format = '{fmt}',
            kinesis_aws_access_key_id = '{AWS_ACCESS_KEY_ID}',
            kinesis_aws_secret_access_key = '{AWS_SECRET_ACCESS_KEY}',
            kinesis_aws_region = '{AWS_REGION}',
            kinesis_endpoint = '{LOCALSTACK_ENDPOINT}',
            kinesis_verify_ssl = 0,
            kinesis_starting_position = 'TRIM_HORIZON',
            kinesis_save_checkpoints = 0
            {', ' + extra_settings if extra_settings else ''}
        """
    )


def wait_for_rows(instance, table_name, expected_count, timeout=30, query=None):
    if query is None:
        query = f"SELECT count() FROM {table_name}"
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = int(instance.query(query).strip())
        if result >= expected_count:
            return result
        time.sleep(0.5)
    raise TimeoutError(
        f"Expected {expected_count} rows in {table_name}, "
        f"got {instance.query(query).strip()} after {timeout}s"
    )


def test_basic_read(started_cluster):
    """Write records to Kinesis via boto3 and read them back through a MV."""
    kinesis = get_kinesis_client()
    stream_name = "test-basic-read"
    create_stream(kinesis, stream_name)

    records = [
        json.dumps({"id": 1, "val": "hello"}),
        json.dumps({"id": 2, "val": "world"}),
    ]
    put_records(kinesis, stream_name, records)

    try:
        create_kinesis_table(
            node, "test_kinesis_basic", stream_name,
            "id UInt64, val String"
        )
        node.query("CREATE TABLE test_kinesis_basic_mv (id UInt64, val String) ENGINE = MergeTree ORDER BY id")
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_basic_mv_view TO test_kinesis_basic_mv "
            "AS SELECT id, val FROM test_kinesis_basic"
        )

        wait_for_rows(node, "test_kinesis_basic_mv", 2)

        result = node.query("SELECT id, val FROM test_kinesis_basic_mv ORDER BY id")
        assert result.strip() == "1\thello\n2\tworld"
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_basic_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_basic_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_basic")
        delete_stream(kinesis, stream_name)


def test_write_to_kinesis(started_cluster):
    """Write to a Kinesis table from ClickHouse and verify records appear in the stream."""
    kinesis = get_kinesis_client()
    stream_name = "test-write"
    create_stream(kinesis, stream_name)

    try:
        create_kinesis_table(
            node, "test_kinesis_write", stream_name,
            "id UInt64, val String"
        )
        node.query("INSERT INTO test_kinesis_write VALUES (10, 'foo'), (20, 'bar')")

        # Read back via a separate reader table
        node.query(f"""
            CREATE TABLE test_kinesis_write_reader (id UInt64, val String)
            ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_aws_access_key_id = '{AWS_ACCESS_KEY_ID}',
                kinesis_aws_secret_access_key = '{AWS_SECRET_ACCESS_KEY}',
                kinesis_aws_region = '{AWS_REGION}',
                kinesis_endpoint = '{LOCALSTACK_ENDPOINT}',
                kinesis_verify_ssl = 0,
                kinesis_starting_position = 'TRIM_HORIZON',
                kinesis_save_checkpoints = 0
        """)
        node.query(
            "CREATE TABLE test_kinesis_write_mv (id UInt64, val String) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_write_mv_view TO test_kinesis_write_mv "
            "AS SELECT id, val FROM test_kinesis_write_reader"
        )

        wait_for_rows(node, "test_kinesis_write_mv", 2)

        result = node.query("SELECT id, val FROM test_kinesis_write_mv ORDER BY id")
        assert result.strip() == "10\tfoo\n20\tbar"
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_write_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_write_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_write_reader")
        node.query("DROP TABLE IF EXISTS test_kinesis_write")
        delete_stream(kinesis, stream_name)


def test_virtual_columns(started_cluster):
    """Verify that virtual columns _sequence_number, _partition_key, _shard_id are populated."""
    kinesis = get_kinesis_client()
    stream_name = "test-virtual-cols"
    create_stream(kinesis, stream_name)

    put_records(kinesis, stream_name, [json.dumps({"n": 42})])

    try:
        create_kinesis_table(
            node, "test_kinesis_virtuals", stream_name,
            "n UInt64"
        )
        node.query(
            "CREATE TABLE test_kinesis_virtuals_mv "
            "(n UInt64, seq String, pk String, shard String) "
            "ENGINE = MergeTree ORDER BY n"
        )
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_virtuals_mv_view TO test_kinesis_virtuals_mv "
            "AS SELECT n, _sequence_number AS seq, _partition_key AS pk, _shard_id AS shard "
            "FROM test_kinesis_virtuals"
        )

        wait_for_rows(node, "test_kinesis_virtuals_mv", 1)

        row = node.query("SELECT seq, pk, shard FROM test_kinesis_virtuals_mv").strip().split("\t")
        assert len(row) == 3
        # Sequence number and shard id should be non-empty
        assert row[0] != ""
        assert row[2].startswith("shardId-")
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_virtuals_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_virtuals_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_virtuals")
        delete_stream(kinesis, stream_name)


def test_csv_format(started_cluster):
    """Test reading CSV-formatted records from Kinesis."""
    kinesis = get_kinesis_client()
    stream_name = "test-csv"
    create_stream(kinesis, stream_name)

    put_records(kinesis, stream_name, ["1,hello", "2,world"])

    try:
        node.query(f"""
            CREATE TABLE test_kinesis_csv (id UInt64, val String)
            ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'CSV',
                kinesis_aws_access_key_id = '{AWS_ACCESS_KEY_ID}',
                kinesis_aws_secret_access_key = '{AWS_SECRET_ACCESS_KEY}',
                kinesis_aws_region = '{AWS_REGION}',
                kinesis_endpoint = '{LOCALSTACK_ENDPOINT}',
                kinesis_verify_ssl = 0,
                kinesis_starting_position = 'TRIM_HORIZON',
                kinesis_save_checkpoints = 0
        """)
        node.query(
            "CREATE TABLE test_kinesis_csv_mv (id UInt64, val String) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_csv_mv_view TO test_kinesis_csv_mv "
            "AS SELECT id, val FROM test_kinesis_csv"
        )

        wait_for_rows(node, "test_kinesis_csv_mv", 2)

        result = node.query("SELECT id, val FROM test_kinesis_csv_mv ORDER BY id")
        assert result.strip() == "1\thello\n2\tworld"
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_csv_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_csv_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_csv")
        delete_stream(kinesis, stream_name)


def test_skip_broken_messages(started_cluster):
    """Broken messages should be skipped when kinesis_skip_broken_messages > 0."""
    kinesis = get_kinesis_client()
    stream_name = "test-broken"
    create_stream(kinesis, stream_name)

    # Mix of valid and invalid JSON records
    records = [
        json.dumps({"id": 1}),
        "not valid json {{{{",
        json.dumps({"id": 2}),
    ]
    put_records(kinesis, stream_name, records)

    try:
        node.query(f"""
            CREATE TABLE test_kinesis_broken (id UInt64)
            ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_aws_access_key_id = '{AWS_ACCESS_KEY_ID}',
                kinesis_aws_secret_access_key = '{AWS_SECRET_ACCESS_KEY}',
                kinesis_aws_region = '{AWS_REGION}',
                kinesis_endpoint = '{LOCALSTACK_ENDPOINT}',
                kinesis_verify_ssl = 0,
                kinesis_starting_position = 'TRIM_HORIZON',
                kinesis_save_checkpoints = 0,
                kinesis_skip_broken_messages = 1
        """)
        node.query(
            "CREATE TABLE test_kinesis_broken_mv (id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_broken_mv_view TO test_kinesis_broken_mv "
            "AS SELECT id FROM test_kinesis_broken"
        )

        wait_for_rows(node, "test_kinesis_broken_mv", 2)

        result = node.query("SELECT id FROM test_kinesis_broken_mv ORDER BY id")
        assert result.strip() == "1\n2"
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_broken_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_broken_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_broken")
        delete_stream(kinesis, stream_name)


def test_multiple_consumers(started_cluster):
    """Test reading from a stream with multiple shards using multiple consumers."""
    kinesis = get_kinesis_client()
    stream_name = "test-multi-consumer"
    shard_count = 2
    create_stream(kinesis, stream_name, shard_count=shard_count)

    records = [json.dumps({"id": i}) for i in range(1, 11)]
    put_records(kinesis, stream_name, records)

    try:
        node.query(f"""
            CREATE TABLE test_kinesis_multi (id UInt64)
            ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_aws_access_key_id = '{AWS_ACCESS_KEY_ID}',
                kinesis_aws_secret_access_key = '{AWS_SECRET_ACCESS_KEY}',
                kinesis_aws_region = '{AWS_REGION}',
                kinesis_endpoint = '{LOCALSTACK_ENDPOINT}',
                kinesis_verify_ssl = 0,
                kinesis_starting_position = 'TRIM_HORIZON',
                kinesis_save_checkpoints = 0,
                kinesis_num_consumers = 2
        """)
        node.query(
            "CREATE TABLE test_kinesis_multi_mv (id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_multi_mv_view TO test_kinesis_multi_mv "
            "AS SELECT id FROM test_kinesis_multi"
        )

        wait_for_rows(node, "test_kinesis_multi_mv", 10)

        result = node.query("SELECT count() FROM test_kinesis_multi_mv")
        assert int(result.strip()) == 10
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_multi_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_multi_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_multi")
        delete_stream(kinesis, stream_name)


def test_trim_horizon_starting_position(started_cluster):
    """TRIM_HORIZON should read all records from the beginning of the stream."""
    kinesis = get_kinesis_client()
    stream_name = "test-trim-horizon"
    create_stream(kinesis, stream_name)

    # Put records before creating the table
    records = [json.dumps({"id": i}) for i in range(1, 4)]
    put_records(kinesis, stream_name, records)

    try:
        node.query(f"""
            CREATE TABLE test_kinesis_trim (id UInt64)
            ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_aws_access_key_id = '{AWS_ACCESS_KEY_ID}',
                kinesis_aws_secret_access_key = '{AWS_SECRET_ACCESS_KEY}',
                kinesis_aws_region = '{AWS_REGION}',
                kinesis_endpoint = '{LOCALSTACK_ENDPOINT}',
                kinesis_verify_ssl = 0,
                kinesis_starting_position = 'TRIM_HORIZON',
                kinesis_save_checkpoints = 0
        """)
        node.query(
            "CREATE TABLE test_kinesis_trim_mv (id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_trim_mv_view TO test_kinesis_trim_mv "
            "AS SELECT id FROM test_kinesis_trim"
        )

        wait_for_rows(node, "test_kinesis_trim_mv", 3)

        result = node.query("SELECT id FROM test_kinesis_trim_mv ORDER BY id")
        assert result.strip() == "1\n2\n3"
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_trim_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_trim_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_trim")
        delete_stream(kinesis, stream_name)


def test_checkpoints(started_cluster):
    """After restart, the engine should resume from saved checkpoints."""
    kinesis = get_kinesis_client()
    stream_name = "test-checkpoints"
    create_stream(kinesis, stream_name)

    records_batch1 = [json.dumps({"id": i}) for i in range(1, 4)]
    put_records(kinesis, stream_name, records_batch1)

    try:
        node.query(f"""
            CREATE TABLE test_kinesis_ckpt (id UInt64)
            ENGINE = Kinesis
            SETTINGS
                kinesis_stream_name = '{stream_name}',
                kinesis_format = 'JSONEachRow',
                kinesis_aws_access_key_id = '{AWS_ACCESS_KEY_ID}',
                kinesis_aws_secret_access_key = '{AWS_SECRET_ACCESS_KEY}',
                kinesis_aws_region = '{AWS_REGION}',
                kinesis_endpoint = '{LOCALSTACK_ENDPOINT}',
                kinesis_verify_ssl = 0,
                kinesis_starting_position = 'TRIM_HORIZON',
                kinesis_save_checkpoints = 1
        """)
        node.query(
            "CREATE TABLE test_kinesis_ckpt_mv (id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        node.query(
            "CREATE MATERIALIZED VIEW test_kinesis_ckpt_mv_view TO test_kinesis_ckpt_mv "
            "AS SELECT id FROM test_kinesis_ckpt"
        )

        wait_for_rows(node, "test_kinesis_ckpt_mv", 3)

        # Detach and re-attach the table to simulate restart
        node.query("DETACH TABLE test_kinesis_ckpt")
        node.query("ATTACH TABLE test_kinesis_ckpt")

        # Put a second batch
        records_batch2 = [json.dumps({"id": i}) for i in range(4, 7)]
        put_records(kinesis, stream_name, records_batch2)

        wait_for_rows(node, "test_kinesis_ckpt_mv", 6)

        result = node.query("SELECT id FROM test_kinesis_ckpt_mv ORDER BY id")
        assert result.strip() == "1\n2\n3\n4\n5\n6"
    finally:
        node.query("DROP VIEW IF EXISTS test_kinesis_ckpt_mv_view")
        node.query("DROP TABLE IF EXISTS test_kinesis_ckpt_mv")
        node.query("DROP TABLE IF EXISTS test_kinesis_ckpt")
        delete_stream(kinesis, stream_name)
