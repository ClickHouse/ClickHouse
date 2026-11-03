import logging
import threading
import time

from kafka import KafkaProducer
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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


# `group_protocol = consumer` (KIP-848) is set for the topics in configs/kip848.xml.
def create_kip848_table(name, topic, flush_interval_ms):
    instance.query(
        f"""
        CREATE TABLE test.{name} (id UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka_sasl:19092',
                     kafka_security_protocol = 'sasl_plaintext',
                     kafka_sasl_mechanism = 'PLAIN',
                     kafka_sasl_username = '{kafka_sasl_user}',
                     kafka_sasl_password = '{kafka_sasl_pass}',
                     kafka_flush_interval_ms = {flush_interval_ms},
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow';
        CREATE MATERIALIZED VIEW test.{name}_mv TO test.messages AS SELECT id FROM test.{name}
            WHERE NOT throwIf((SELECT count() FROM test.fail_inserts) > 0);
        """
    )


def wait_kafka_consumer(name, condition):
    assert_eq_with_retry(
        instance,
        f"SELECT {condition} FROM system.kafka_consumers WHERE table = '{name}'",
        "1",
        retry_count=120,
        sleep_time=0.5,
    )


def wait_assigned_partitions(name, condition):
    wait_kafka_consumer(name, f"sum(length(assignments.partition_id)) {condition}")


# The broker assigns the partitions, and every rebalance is incremental. Members joining and leaving the group
# must not lose or stall messages.
def test_kafka_kip848_consumer_group_protocol(kafka_cluster):
    kafka_cli(kafka_cluster, "kafka-topics", "--create", "--topic", "kip848", "--partitions", "6")

    def create_kafka_table(name):
        create_kip848_table(name, "kip848", 500)

    producer = get_kafka_producer(kafka_cluster.kafka_sasl_port)
    produced = 0

    def produce(count):
        nonlocal produced
        for i in range(produced, produced + count):
            producer.send(topic="kip848", value=f'{{"id":{i}}}', partition=i % 6)
        producer.flush()
        produced += count

    def produce_and_wait(count):
        produce(count)
        assert_eq_with_retry(
            instance, "SELECT uniqExact(id) FROM test.messages", str(produced), retry_count=120, sleep_time=0.5
        )

    instance.query("CREATE TABLE test.messages (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.fail_inserts (x UInt8) ENGINE = Memory")
    create_kafka_table("kafka1")
    produce_and_wait(1000)
    wait_assigned_partitions("kafka1", "= 6")

    # The new member takes a part of the partitions, the first one keeps the rest.
    create_kafka_table("kafka2")
    wait_assigned_partitions("kafka2", "> 0")
    produce_and_wait(1000)

    # And gets them back when the new member leaves.
    instance.query("DROP TABLE test.kafka2_mv; DROP TABLE test.kafka2 SYNC")
    wait_assigned_partitions("kafka1", "= 6")
    produce_and_wait(1000)

    # The messages of a failed insert are read again, not skipped.
    errors_query = "SELECT sum(value) FROM system.errors WHERE name = 'FUNCTION_THROW_IF_VALUE_IS_NON_ZERO'"
    errors = int(instance.query(errors_query))
    instance.query("INSERT INTO test.fail_inserts VALUES (1)")
    produce(1000)
    assert_eq_with_retry(instance, f"SELECT ({errors_query}) > {errors}", "1", retry_count=120, sleep_time=0.5)
    instance.query("TRUNCATE TABLE test.fail_inserts")
    produce_and_wait(0)
    producer.close()

    assert instance.query("SELECT min(id), max(id) FROM test.messages") == f"0\t{produced - 1}\n"
    assert int(instance.query("SELECT num_rebalance_revocations FROM system.kafka_consumers WHERE table = 'kafka1'")) > 0
    assert "kip848" in kafka_cli(kafka_cluster, "kafka-groups", "--list", "--group-type", "consumer")


# A partial revocation drops the block being formed, including the rows already read from the partitions the
# consumer keeps. Those partitions must be rewound, otherwise the rows are skipped and the next commit loses them.
def test_kafka_kip848_partial_revocation_rewinds_unfinished_block(kafka_cluster):
    kafka_cli(kafka_cluster, "kafka-topics", "--create", "--topic", "kip848_rewind", "--partitions", "6")
    instance.query("CREATE TABLE test.messages (id UInt64) ENGINE = MergeTree ORDER BY id")
    instance.query("CREATE TABLE test.fail_inserts (x UInt8) ENGINE = Memory")

    # Long blocks, so that the revocation arrives while one is being formed.
    flush_interval_ms = 15000
    create_kip848_table("kafka1", "kip848_rewind", flush_interval_ms)
    wait_assigned_partitions("kafka1", "= 6")

    # Produce all the time, so that every block has rows from every partition.
    producer = get_kafka_producer(kafka_cluster.kafka_sasl_port)
    produced = 0
    stop = threading.Event()

    def produce():
        nonlocal produced
        while not stop.is_set():
            for _ in range(6):
                producer.send(topic="kip848_rewind", value=f'{{"id":{produced}}}', partition=produced % 6)
                produced += 1
            producer.flush()
            time.sleep(0.05)

    producer_thread = threading.Thread(target=produce)
    producer_thread.start()
    try:
        # Join right after a block was committed: the revocation then arrives in the middle of the next block.
        commits = int(instance.query("SELECT num_commits FROM system.kafka_consumers WHERE table = 'kafka1'"))
        wait_kafka_consumer("kafka1", f"num_commits > {commits}")
        rejected_query = "SELECT sum(value) FROM system.events WHERE name = 'KafkaRowsRejected'"
        rejected = int(instance.query(rejected_query))
        create_kip848_table("kafka2", "kip848_rewind", flush_interval_ms)
        wait_assigned_partitions("kafka2", "> 0")
        wait_assigned_partitions("kafka1", "> 0")
        wait_kafka_consumer("kafka1", "num_rebalance_revocations > 0")
        assert_eq_with_retry(instance, f"SELECT ({rejected_query}) > {rejected}", "1", retry_count=120, sleep_time=0.5)
        # Commits after the rebalance must not skip the rewound rows.
        commits = int(instance.query("SELECT num_commits FROM system.kafka_consumers WHERE table = 'kafka1'"))
        wait_kafka_consumer("kafka1", f"num_commits > {commits}")
    finally:
        stop.set()
        producer_thread.join()
        producer.close()

    assert_eq_with_retry(
        instance, "SELECT uniqExact(id) FROM test.messages", str(produced), retry_count=120, sleep_time=0.5
    )
    assert instance.query("SELECT min(id), max(id) FROM test.messages") == f"0\t{produced - 1}\n"
