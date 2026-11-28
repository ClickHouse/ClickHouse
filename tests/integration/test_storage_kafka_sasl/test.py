import json
import logging
import time
import uuid

from kafka import KafkaProducer
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, wait_condition

from helpers.config_cluster import kafka_sasl_user, kafka_sasl_pass

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/sasl_settings.xml", "configs/kip848.xml"],
    with_kafka_sasl=True,
    stay_alive=True,
)


def get_kafka_producer(port):
    errors = []
    for _ in range(15):
        try:
            producer = KafkaProducer(
                bootstrap_servers = "localhost:{}".format(port),
                security_protocol = "SASL_PLAINTEXT",
                sasl_mechanism    = "PLAIN",
                sasl_plain_username = kafka_sasl_user,
                sasl_plain_password = kafka_sasl_pass,
                value_serializer = lambda v: v.encode('utf-8'),
            )
            logging.debug("Kafka Connection established: localhost:{}".format(port))
            return producer
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)

    raise Exception("Connection not established, {}".format(errors))

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_sasl_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster
    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test; CREATE DATABASE test;")
    yield

def test_kafka_sasl(kafka_cluster):
    instance.query(
        f"""
        DROP DATABASE IF EXISTS test SYNC;
        CREATE DATABASE test;

        CREATE TABLE test.kafka_sasl (key int, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka_sasl:19092',
                     kafka_security_protocol = 'sasl_plaintext',
                     kafka_sasl_mechanism = 'PLAIN',
                     kafka_sasl_username = '{kafka_sasl_user}',
                     kafka_sasl_password = '{kafka_sasl_pass}',
                     kafka_flush_interval_ms = 1000,
                     kafka_topic_list = 'topic1',
                     kafka_group_name = 'group1',
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.messages (key int, value String) ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.kafka_consumer TO test.messages (key int, value String)
        AS SELECT key, value FROM test.kafka_sasl;
        """
    )

    producer = get_kafka_producer(kafka_cluster.kafka_sasl_port)
    producer.send(topic="topic1", value='{"key":1, "value":"test123"}')
    producer.flush()
    producer.close()

    assert_eq_with_retry(
        instance,
        "SELECT value FROM test.messages",
        "test123",
        retry_count=120,
        sleep_time=0.5,
    )


def test_kafka_sasl_settings_precedence(kafka_cluster):
    # Test that SASL related settings in create query override those in config
    config_with_wrong_passwords = """
    <clickhouse>
    <kafka>
        <security_protocol>plaintext</security_protocol>
        <sasl_mechanism>SCRAM-SHA-256</sasl_mechanism>
        <sasl_username>wrong_user</sasl_username>
        <sasl_password>wrong_password</sasl_password>
        <consumer>
            <security_protocol>ssl</security_protocol>
            <sasl_mechanism>SCRAM-SHA-512</sasl_mechanism>
            <sasl_username>wrong_user_2</sasl_username>
            <sasl_password>wrong_password_2</sasl_password>
        </consumer>
    </kafka>
</clickhouse>
"""
    config_file_path = "/etc/clickhouse-server/config.d/sasl_settings.xml"

    try:
        with instance.with_replace_config(config_file_path, config_with_wrong_passwords):
            instance.restart_clickhouse()
            test_kafka_sasl(kafka_cluster)
    finally:
        instance.restart_clickhouse()


# ClickHouse may crash when the table is created with an option to put
# broken messages to a dead letter queue, while the table
# system.dead_letter_queue is not configured.
def test_dead_letter_segfault(kafka_cluster):
    res = instance.query_and_get_error(
        f"""
        DROP DATABASE IF EXISTS segfault SYNC;
        CREATE DATABASE segfault;

        CREATE TABLE segfault.kafka_sasl (key int, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka_sasl:19092',
                     kafka_security_protocol = 'sasl_plaintext',
                     kafka_sasl_mechanism = 'PLAIN',
                     kafka_sasl_username = '{kafka_sasl_user}',
                     kafka_sasl_password = '{kafka_sasl_pass}',
                     kafka_topic_list = 'topic1',
                     kafka_group_name = 'group1',
                     kafka_format = 'JSONEachRow',
                     kafka_handle_error_mode = 'dead_letter_queue';
        """
    )

    assert "The table system.dead_letter_queue is not configured on the server" in res


def kafka_cli(kafka_cluster, tool, *args):
    config = "/tmp/kip848_client.properties"
    properties = (
        "security.protocol=SASL_PLAINTEXT\n"
        "sasl.mechanism=PLAIN\n"
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{kafka_sasl_user}" password="{kafka_sasl_pass}";\n'
    )
    kafka_cluster.exec_in_container(
        kafka_cluster.kafka_sasl_docker_id, ["bash", "-c", f"echo '{properties}' > {config}"]
    )
    return kafka_cluster.exec_in_container(
        kafka_cluster.kafka_sasl_docker_id,
        [tool, "--bootstrap-server", "kafka_sasl:19092", "--command-config", config, *args],
    )


@pytest.fixture
def kip848_topic(kafka_cluster):
    topic = f"kip848_{uuid.uuid4().hex}"
    kafka_cli(kafka_cluster, "kafka-topics", "--create", "--topic", topic, "--partitions", "2")
    producer = get_kafka_producer(kafka_cluster.kafka_sasl_port)

    def produce(*ids):
        for i in ids:
            producer.send(topic, value=f'{{"id":{i}}}', partition=i % 2).get(timeout=15)

    try:
        # Both partitions have history that a new group using `latest` must skip.
        produce(0, 1)
        instance.query(
            "CREATE TABLE test.messages (id UInt64, owner String) ENGINE=Memory;"
            "CREATE TABLE test.fail_inserts (x UInt8) ENGINE=Memory"
        )
        yield topic, produce
    finally:
        producer.close()


def create_kip848_member(name, topic, flush_interval_ms=500, commit_every_batch=0):
    instance.query(
        f"""
        CREATE TABLE test.{name} (id UInt64)
            ENGINE = Kafka(kip848)
            SETTINGS kafka_broker_list = 'kafka_sasl:19092',
                     kafka_security_protocol = 'sasl_plaintext',
                     kafka_sasl_mechanism = 'PLAIN',
                     kafka_sasl_username = '{kafka_sasl_user}',
                     kafka_sasl_password = '{kafka_sasl_pass}',
                     kafka_flush_interval_ms = {flush_interval_ms},
                     kafka_poll_max_batch_size = 1,
                     kafka_commit_every_batch = {commit_every_batch},
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow';
        CREATE MATERIALIZED VIEW test.{name}_mv TO test.messages AS
            SELECT id, '{name}' AS owner FROM test.{name}
            WHERE NOT throwIf(id = 10 AND (SELECT count() FROM test.fail_inserts) > 0);
        """
    )


def wait_assigned_partitions(name, count):
    assert_eq_with_retry(
        instance,
        f"SELECT length(assignments.partition_id) FROM system.kafka_consumers WHERE database='test' AND table='{name}'",
        str(count), retry_count=120, sleep_time=0.5,
    )


def wait_for_latest(topic):
    # Assignment is visible before librdkafka resolves `latest`. Wait until both fetchers
    # reached the end of the pre-existing records before producing anything to be delivered.
    def positions():
        value = instance.query("SELECT rdkafka_stat FROM system.kafka_consumers WHERE database='test' AND table='kafka1' FORMAT TSVRaw")
        if not value.strip():
            return []
        partitions = json.loads(value)["topics"].get(topic, {}).get("partitions", {})
        return [partitions[p]["next_offset"] for p in ("0", "1") if p in partitions]

    wait_condition(positions, lambda offsets: offsets == [1, 1], max_attempts=120, delay=0.5)


@pytest.mark.parametrize("commit_every_batch", [0, 1])
def test_kafka_kip848_first_insert_failure(kip848_topic, commit_every_batch):
    topic, produce = kip848_topic
    instance.query("INSERT INTO test.fail_inserts VALUES (1)")
    create_kip848_member("kafka1", topic, commit_every_batch=commit_every_batch)
    wait_for_latest(topic)

    errors_query = "SELECT sum(value) FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'"
    errors = int(instance.query(errors_query))
    produce(10)
    assert_eq_with_retry(instance, f"SELECT ({errors_query}) > {errors}", "1", retry_count=120, sleep_time=0.5)
    assert instance.query("SELECT count() FROM test.messages") == "0\n"

    instance.query("TRUNCATE TABLE test.fail_inserts")
    produce(12)
    assert_eq_with_retry(instance, "SELECT arraySort(groupArray(id)) FROM test.messages", "[10,12]", retry_count=120)


def test_kafka_kip848_partial_revocation_rewinds_unfinished_block(kafka_cluster, kip848_topic):
    topic, produce = kip848_topic
    failpoint = "kafka_consumer_pause_mid_block"
    create_kip848_member("kafka1", topic, flush_interval_ms=60000)
    wait_for_latest(topic)

    def block_holds_both_rows():
        instance.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=30)
        positions = instance.query(
            "SELECT arraySort(assignments.current_offset) FROM system.kafka_consumers WHERE database='test' AND table='kafka1'"
        )
        if positions == "[2,2]\n":
            return True
        instance.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
        return False

    def member_partitions():
        out = kafka_cli(kafka_cluster, "kafka-consumer-groups", "--describe", "--group", topic, "--members")
        return sorted(int(line.split()[-1]) for line in out.splitlines() if line.startswith(topic + " "))

    # Hold the two rows in one unfinished block until the coordinator requests a partial revocation.
    instance.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    try:
        produce(10, 11)
        wait_condition(block_holds_both_rows, bool, max_attempts=8)
        assert instance.query("SELECT count() FROM test.messages") == "0\n"
        create_kip848_member("kafka2", topic)
        wait_condition(member_partitions, lambda counts: counts == [0, 1], max_attempts=30, delay=0.5)
    finally:
        instance.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

    # One row must be replayed by the original owner, the other by the new owner.
    assert_eq_with_retry(
        instance, "SELECT arraySort(groupArray(id)), uniqExact(owner) FROM test.messages", "[10,11]\t2",
        retry_count=120, sleep_time=0.5,
    )
    assert topic in kafka_cli(kafka_cluster, "kafka-groups", "--list", "--group-type", "consumer")

    instance.query("DROP TABLE test.kafka2_mv; DROP TABLE test.kafka2 SYNC")
    wait_assigned_partitions("kafka1", 2)
    produce(20, 21)
    assert_eq_with_retry(
        instance, "SELECT arraySort(groupArray(id)) FROM test.messages WHERE owner='kafka1' AND id >= 20",
        "[20,21]", retry_count=120, sleep_time=0.5,
    )
