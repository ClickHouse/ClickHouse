"""Tests for AvroConfluent format with the Kafka table engine and Schema Registry.

Verifies that ClickHouse can produce AvroConfluent-framed messages into Kafka
(registering schemas automatically) and consume them back, round-tripping data
through the Confluent Schema Registry.
"""

import struct

import avro.schema
import pytest
from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient,
)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from helpers.cluster import ClickHouseCluster, is_arm
import helpers.kafka.common as k

if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(f"kafka_id is {kafka_id}")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield


def schema_registry_url_internal(kafka_cluster):
    """Registry URL reachable from inside the Docker network (for ClickHouse)."""
    return "http://{}:{}".format(
        kafka_cluster.schema_registry_host, kafka_cluster.schema_registry_port
    )


def schema_registry_url_external(kafka_cluster):
    """Registry URL reachable from the test host."""
    return "http://localhost:{}".format(kafka_cluster.schema_registry_port)


def get_schema_registry_client(kafka_cluster):
    return CachedSchemaRegistryClient(
        {"url": schema_registry_url_external(kafka_cluster)}
    )


def test_kafka_produce_consume_avro_confluent(kafka_cluster):
    """Write rows via Kafka engine with AvroConfluent output, read them back."""

    topic_name = f"avro_confluent_roundtrip_{k.random_string(6)}"
    admin_client = k.get_admin_client(kafka_cluster)
    sr_url = schema_registry_url_internal(kafka_cluster)

    with k.kafka_topic(admin_client, topic_name):
        # Writer table: ClickHouse -> Kafka in AvroConfluent format.
        instance.query(f"""
            CREATE TABLE test.kafka_avro_writer (id Int64, name String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}_writer',
                         kafka_format = 'AvroConfluent',
                         format_avro_schema_registry_url = '{sr_url}',
                         output_format_avro_confluent_subject = '{topic_name}-value',
                         output_format_avro_string_column_pattern = '^name$'
        """)

        # Insert rows through the Kafka writer table.
        instance.query(
            "INSERT INTO test.kafka_avro_writer VALUES "
            "(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
        )

        # Reader table: Kafka -> ClickHouse in AvroConfluent format.
        instance.query(f"""
            CREATE TABLE test.kafka_avro_reader (id Int64, name String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}_reader',
                         kafka_format = 'AvroConfluent',
                         format_avro_schema_registry_url = '{sr_url}'
        """)

        # Materialized view to persist consumed rows.
        instance.query("""
            CREATE TABLE test.avro_sink (id Int64, name String)
                ENGINE = MergeTree ORDER BY id
        """)
        instance.query("""
            CREATE MATERIALIZED VIEW test.avro_mv TO test.avro_sink AS
                SELECT * FROM test.kafka_avro_reader
        """)

        result = instance.query_with_retry(
            "SELECT id, name FROM test.avro_sink ORDER BY id",
            check_callback=lambda res: res.count("\n") == 3,
            retry_count=60,
            sleep_time=1,
        )

        assert result.strip() == "1\tAlice\n2\tBob\n3\tCharlie"


def test_kafka_consume_avro_confluent_from_python(kafka_cluster):
    """Produce AvroConfluent messages from Python, consume via Kafka engine."""

    topic_name = f"avro_confluent_py_produce_{k.random_string(6)}"
    admin_client = k.get_admin_client(kafka_cluster)
    sr_url = schema_registry_url_internal(kafka_cluster)
    sr_client = get_schema_registry_client(kafka_cluster)
    serializer = MessageSerializer(sr_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_py_produce",
            "type": "record",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "value", "type": "string"},
            ],
        }
    )

    with k.kafka_topic(admin_client, topic_name):
        # Produce three Confluent-framed messages via the Python serializer.
        messages = []
        for i in range(3):
            msg = serializer.encode_record_with_schema(
                f"{topic_name}-value", schema, {"id": i, "value": f"row_{i}"}
            )
            messages.append(msg)

        k.kafka_produce(kafka_cluster, topic_name, messages)

        # Consume via Kafka engine.
        instance.query(f"""
            CREATE TABLE test.kafka_py_avro (id Int64, value String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}_group',
                         kafka_format = 'AvroConfluent',
                         format_avro_schema_registry_url = '{sr_url}'
        """)

        instance.query("""
            CREATE TABLE test.py_avro_sink (id Int64, value String)
                ENGINE = MergeTree ORDER BY id
        """)
        instance.query("""
            CREATE MATERIALIZED VIEW test.py_avro_mv TO test.py_avro_sink AS
                SELECT * FROM test.kafka_py_avro
        """)

        result = instance.query_with_retry(
            "SELECT id, value FROM test.py_avro_sink ORDER BY id",
            check_callback=lambda res: res.count("\n") == 3,
            retry_count=60,
            sleep_time=1,
        )

        assert result.strip() == "0\trow_0\n1\trow_1\n2\trow_2"


def test_kafka_produce_avro_confluent_verify_wire_format(kafka_cluster):
    """Produce via Kafka engine and verify the Confluent wire format with Python."""

    topic_name = f"avro_confluent_wire_{k.random_string(6)}"
    admin_client = k.get_admin_client(kafka_cluster)
    sr_url = schema_registry_url_internal(kafka_cluster)

    with k.kafka_topic(admin_client, topic_name):
        instance.query(f"""
            CREATE TABLE test.kafka_wire_writer (x Int64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}_writer',
                         kafka_format = 'AvroConfluent',
                         format_avro_schema_registry_url = '{sr_url}',
                         output_format_avro_confluent_subject = '{topic_name}-value'
        """)

        instance.query("INSERT INTO test.kafka_wire_writer VALUES (42), (100)")

        # Consume raw messages from Kafka with the Python consumer and verify
        # the Confluent wire format: [0x00][schema_id 4B BE][avro datum].
        messages = k.kafka_consume_with_retry(
            kafka_cluster, topic_name, expected_messages=2, need_decode=False
        )

        sr_client = get_schema_registry_client(kafka_cluster)
        serializer = MessageSerializer(sr_client)

        decoded_values = []
        for raw in messages:
            # Verify magic byte.
            assert raw[0:1] == b"\x00", f"Expected magic byte 0x00, got {raw[0:1]!r}"
            # Decode schema_id.
            schema_id = struct.unpack(">I", raw[1:5])[0]
            assert schema_id > 0, f"Expected positive schema_id, got {schema_id}"

            # Decode the full message via the Python deserializer.
            decoded = serializer.decode_message(raw)
            decoded_values.append(decoded["x"])

        assert sorted(decoded_values) == [42, 100]


def test_kafka_avro_confluent_multiple_rows_per_message(kafka_cluster):
    """Verify that kafka_max_rows_per_message packs multiple Confluent-framed
    records into one Kafka message, and that the consumer unpacks them."""

    topic_name = f"avro_confluent_multi_{k.random_string(6)}"
    admin_client = k.get_admin_client(kafka_cluster)
    sr_url = schema_registry_url_internal(kafka_cluster)

    with k.kafka_topic(admin_client, topic_name):
        instance.query(f"""
            CREATE TABLE test.kafka_multi_writer (n Int64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}_writer',
                         kafka_format = 'AvroConfluent',
                         kafka_max_rows_per_message = 5,
                         format_avro_schema_registry_url = '{sr_url}',
                         output_format_avro_confluent_subject = '{topic_name}-value'
        """)

        # Insert 10 rows; with max_rows_per_message = 5 this should produce 2 messages.
        values = ", ".join(f"({i})" for i in range(10))
        instance.query(f"INSERT INTO test.kafka_multi_writer VALUES {values}")

        # Read them back.
        instance.query(f"""
            CREATE TABLE test.kafka_multi_reader (n Int64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}_reader',
                         kafka_format = 'AvroConfluent',
                         format_avro_schema_registry_url = '{sr_url}'
        """)
        instance.query("""
            CREATE TABLE test.multi_sink (n Int64) ENGINE = MergeTree ORDER BY n
        """)
        instance.query("""
            CREATE MATERIALIZED VIEW test.multi_mv TO test.multi_sink AS
                SELECT * FROM test.kafka_multi_reader
        """)

        result = instance.query_with_retry(
            "SELECT n FROM test.multi_sink ORDER BY n",
            check_callback=lambda res: res.count("\n") == 10,
            retry_count=60,
            sleep_time=1,
        )

        expected = "\n".join(str(i) for i in range(10))
        assert result.strip() == expected


def test_kafka_avro_confluent_schema_id_stable_across_inserts(kafka_cluster):
    """Verify that dropping and recreating the writer table (which creates a new
    format instance and re-registers the schema) produces the same schema ID."""

    topic_name = f"avro_confluent_stable_id_{k.random_string(6)}"
    admin_client = k.get_admin_client(kafka_cluster)
    sr_url = schema_registry_url_internal(kafka_cluster)

    def create_writer():
        instance.query(f"""
            CREATE TABLE test.kafka_stable_writer (id Int64, name String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                         kafka_topic_list = '{topic_name}',
                         kafka_group_name = '{topic_name}_writer',
                         kafka_format = 'AvroConfluent',
                         format_avro_schema_registry_url = '{sr_url}',
                         output_format_avro_confluent_subject = '{topic_name}-value',
                         output_format_avro_string_column_pattern = '^name$'
        """)

    with k.kafka_topic(admin_client, topic_name):
        # First insert.
        create_writer()
        instance.query(
            "INSERT INTO test.kafka_stable_writer VALUES (1, 'Alice'), (2, 'Bob')"
        )
        instance.query("DROP TABLE test.kafka_stable_writer")

        # Second insert through a fresh table (new format instance).
        create_writer()
        instance.query(
            "INSERT INTO test.kafka_stable_writer VALUES (3, 'Charlie')"
        )
        instance.query("DROP TABLE test.kafka_stable_writer")

        # Consume all raw messages and extract schema IDs.
        messages = k.kafka_consume_with_retry(
            kafka_cluster, topic_name, expected_messages=3, need_decode=False
        )

        schema_ids = set()
        for raw in messages:
            assert raw[0:1] == b"\x00"
            schema_ids.add(struct.unpack(">I", raw[1:5])[0])

        assert len(schema_ids) == 1, (
            f"Expected all messages to share the same schema ID, got {schema_ids}"
        )
