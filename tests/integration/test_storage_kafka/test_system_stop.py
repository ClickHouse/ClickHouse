"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on a Kafka table."""

from helpers.kafka.common_direct import *
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_topic_old": k.KAFKA_TOPIC_OLD,
        "kafka_group_name_old": k.KAFKA_CONSUMER_GROUP_OLD,
        "kafka_topic_new": k.KAFKA_TOPIC_NEW,
        "kafka_group_name_new": k.KAFKA_CONSUMER_GROUP_NEW,
        "kafka_client_id": "instance",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    clickhouse_path_dir="clickhouse_path",
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin_client = k.get_admin_client(cluster)

    topics = [t for t in admin_client.list_topics() if not t.startswith("_")]
    logging.debug(f"Deleting topics: {topics}")
    result = admin_client.delete_topics(topics)
    for topic, error in result.topic_error_codes:
        if error != 0:
            logging.warning(f"Received error {error} while deleting topic {topic}")
        else:
            logging.info(f"Deleted topic {topic}")

    yield


def setup_consuming_table(table, topic):
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow',
                     kafka_flush_interval_ms = 500;

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )


def produce(kafka_cluster, topic, start, count):
    messages = [
        json.dumps({"key": i, "value": i}) for i in range(start, start + count)
    ]
    k.kafka_produce(kafka_cluster, topic, messages)


def wait_dst_count(table, expected):
    instance.query_with_retry(
        f"SELECT count() FROM test.{table}_dst",
        check_callback=lambda res: int(res) == expected,
        retry_count=120,
        sleep_time=0.5,
    )


def assert_dst_count_stable(table, expected, seconds=10):
    """The consumer is expected to be stopped, so the row count must not grow."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(instance.query(f"SELECT count() FROM test.{table}_dst")) == expected
        time.sleep(1)


def test_system_stop_start_consuming(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_stop_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # STOP halts consumption: new messages stay in the topic, unread.
        instance.query(f"SYSTEM STOP test.{table}")
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        # START resumes consumption and the backlog is drained.
        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 20)


def test_system_pause_start_consuming(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_pause_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # PAUSE stops further activity: subsequent messages are not consumed.
        instance.query(f"SYSTEM PAUSE test.{table}")
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        instance.query(f"SYSTEM START test.{table}")
        wait_dst_count(table, 20)


def test_system_cancel_consuming(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_cancel_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # CANCEL interrupts only the current poll; it does NOT stop further activity,
        # so consumption keeps going and later messages are still ingested.
        instance.query(f"SYSTEM CANCEL test.{table}")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


def test_system_refresh_consuming(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refresh_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # REFRESH kicks off a poll out of order; streaming continues normally.
        instance.query(f"SYSTEM REFRESH test.{table}")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


def test_system_stop_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_allbg_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # The server-wide wildcard halts consumption for every streaming table.
        instance.query("SYSTEM STOP ALL BACKGROUND")
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        # START ALL BACKGROUND resumes every streaming table.
        instance.query("SYSTEM START ALL BACKGROUND")
        wait_dst_count(table, 20)


def test_system_pause_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_pauseall_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # PAUSE ALL BACKGROUND halts consumption for every streaming table.
        instance.query("SYSTEM PAUSE ALL BACKGROUND")
        produce(kafka_cluster, table, 10, 10)
        assert_dst_count_stable(table, 10)

        instance.query("SYSTEM START ALL BACKGROUND")
        wait_dst_count(table, 20)


def test_system_cancel_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_cancelall_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # CANCEL ALL BACKGROUND interrupts the current poll but does not halt the stream.
        instance.query("SYSTEM CANCEL ALL BACKGROUND")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


def test_system_refresh_all_background(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_refreshall_{k.random_string(6)}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)

        produce(kafka_cluster, table, 0, 10)
        wait_dst_count(table, 10)

        # REFRESH ALL BACKGROUND kicks off a poll for every streaming table; the stream continues.
        instance.query("SYSTEM REFRESH ALL BACKGROUND")
        produce(kafka_cluster, table, 10, 10)
        wait_dst_count(table, 20)


def test_system_stop_requires_grant(kafka_cluster):
    admin_client = k.get_admin_client(kafka_cluster)
    table = f"kafka_grant_{k.random_string(6)}"
    user = f"user_{table}"
    with k.kafka_topic(admin_client, table):
        setup_consuming_table(table, table)
        instance.query(f"DROP USER IF EXISTS {user}; CREATE USER {user}")

        # A user without the required SYSTEM privilege cannot control the table.
        for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
            assert "ACCESS_DENIED" in instance.query_and_get_error(
                f"SYSTEM {verb} test.{table}", user=user
            )

        # Once granted, every verb succeeds.
        instance.query(f"GRANT SYSTEM ON *.* TO {user}")
        for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
            instance.query(f"SYSTEM {verb} test.{table}", user=user)

        instance.query(f"DROP USER {user}")
