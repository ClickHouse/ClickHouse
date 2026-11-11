import time

import pytest

from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/small_message_broker_pool.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,  # For Kafka2
    macros={
        "kafka_broker": "kafka1",
        "kafka_format_json_each_row": "JSONEachRow",
    },
    stay_alive=True,
)

STARTUP_POOL_SIZE = 2
RELOADED_POOL_SIZE = 8
NUM_TABLES = 4
OVERRIDE_PATH = "/etc/clickhouse-server/config.d/zz_message_broker_pool_size.xml"


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield
    # The pool size can only grow at runtime, so a restart is needed to get back to the startup size.
    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.exec_in_container(["bash", "-c", f"rm -f {OVERRIDE_PATH}"])
    instance.restart_clickhouse()


def metric(name):
    return int(
        instance.query(
            f"SELECT value FROM system.metrics WHERE metric = '{name}'"
        ).strip()
    )


def max_executing_tasks(duration_sec, stop_at=None):
    # Only the Kafka tables of this test run tasks in the message broker pool.
    deadline = time.monotonic() + duration_sec
    result = 0
    while time.monotonic() < deadline:
        result = max(result, metric("BackgroundMessageBrokerSchedulePoolTask"))
        if stop_at is not None and result >= stop_at:
            break
        time.sleep(0.1)
    return result


def pool_size_metric():
    return metric("BackgroundMessageBrokerSchedulePoolSize")


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_kafka_tasks_use_increased_message_broker_pool(
    kafka_cluster, create_query_generator
):
    admin_client = k.get_admin_client(kafka_cluster)
    postfix = k.get_topic_postfix(create_query_generator)
    topics = [f"pool_resize_{postfix}_{i}" for i in range(NUM_TABLES)]
    for topic in topics:
        k.kafka_create_topic(admin_client, topic)

    # The topics stay empty, so every streaming task spends most of its time
    # blocked in poll, holding a pool thread.
    for i, topic in enumerate(topics):
        create_query = create_query_generator(
            f"kafka_{i}",
            "key UInt64, value UInt64",
            topic_list=topic,
            consumer_group=topic,
            settings={
                "kafka_thread_per_consumer": k.must_use_thread_per_consumer(
                    create_query_generator
                ),
                "kafka_consumer_reschedule_ms": 100,
            },
        )
        instance.query(f"""
            {create_query};
            CREATE TABLE test.view_{i} (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
            CREATE MATERIALIZED VIEW test.consumer_{i} TO test.view_{i} AS SELECT * FROM test.kafka_{i};
            """)

    assert pool_size_metric() == STARTUP_POOL_SIZE
    assert max_executing_tasks(30, stop_at=STARTUP_POOL_SIZE) == STARTUP_POOL_SIZE
    assert max_executing_tasks(5) == STARTUP_POOL_SIZE

    instance.replace_config(
        OVERRIDE_PATH,
        f"<clickhouse><background_message_broker_schedule_pool_size>{RELOADED_POOL_SIZE}</background_message_broker_schedule_pool_size></clickhouse>",
    )
    instance.query("SYSTEM RELOAD CONFIG")
    assert pool_size_metric() == RELOADED_POOL_SIZE

    # Before the fix the tasks of one type stayed capped at the startup pool size.
    assert max_executing_tasks(60, stop_at=NUM_TABLES) == NUM_TABLES
