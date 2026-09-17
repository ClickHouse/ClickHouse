from helpers.kafka.common_direct import *
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    with_kafka=True,
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


def test_system_kafka_consumers_grant(kafka_cluster, max_retries=20):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    k.kafka_create_topic(admin_client, "visible")
    k.kafka_create_topic(admin_client, "hidden")
    instance.query(
        f"""
        DROP TABLE IF EXISTS kafka_grant_visible;
        DROP TABLE IF EXISTS kafka_grant_hidden;

        CREATE TABLE kafka_grant_visible (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'visible',
                     kafka_group_name = 'visible',
                     kafka_format = 'JSONEachRow',
                     kafka_flush_interval_ms=1000,
                     kafka_num_consumers = 1;

        CREATE TABLE kafka_grant_hidden (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'hidden',
                     kafka_group_name = 'hidden',
                     kafka_format = 'JSONEachRow',
                     kafka_flush_interval_ms=1000,
                     kafka_num_consumers = 1;
    """
    )

    result_system_kafka_consumers = instance.query_with_retry(
        """
        SELECT count(1) FROM system.kafka_consumers WHERE table LIKE 'kafka_grant%'
        """,
        retry_count=max_retries,
        sleep_time=1,
        check_callback=lambda res: int(res) == 2,
    )
    # both kafka_grant_hidden and kafka_grant_visible tables are visible

    instance.query(
        f"""
        DROP USER IF EXISTS restricted;
        CREATE USER restricted;
        GRANT SHOW ON default.kafka_grant_visible TO restricted;
        GRANT SELECT ON system.kafka_consumers TO restricted;
    """
    )

    restricted_result_system_kafka_consumers = instance.query(
        "SELECT count(1) FROM system.kafka_consumers WHERE table LIKE 'kafka_grant%'",
        user="restricted",
    )
    assert int(restricted_result_system_kafka_consumers) == 1
    # only kafka_grant_visible is visible for user `restricted`

    k.kafka_delete_topic(admin_client, "visible")
    k.kafka_delete_topic(admin_client, "hidden")
    instance.query(
        f"""
        DROP TABLE IF EXISTS kafka_grant_visible;
        DROP TABLE IF EXISTS kafka_grant_hidden;
        DROP USER IF EXISTS restricted;
    """
    )


def test_log_to_exceptions(kafka_cluster, max_retries=20):

    non_existent_broker_port = 9876
    instance.query(
        f"""
        DROP TABLE IF EXISTS foo_exceptions;

        CREATE TABLE foo_exceptions(a String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'localhost:{non_existent_broker_port}', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'RawBLOB';
    """
    )

    instance.query(
        "SELECT * FROM foo_exceptions SETTINGS stream_like_engine_allow_direct_select=1"
    )
    instance.query("SYSTEM FLUSH LOGS")

    # `librdkafka` emits several log lines when the broker is unreachable.
    # Both flow into `system.kafka_consumers.exceptions` via
    # `KafkaConsumer::setExceptionInfo`:
    #   - per-attempt `Connect to ... failed: Connection refused`
    #   - periodic `N/M brokers are down` summary
    # There is no guarantee that any specific line shows up first or at all
    # under unusual broker-thread timing, so accept either form.
    thrd_prefix = (
        f"[thrd:localhost:{non_existent_broker_port}/bootstrap]:"
    )
    broker_down_marker = f"{thrd_prefix} 1/1 brokers are down"
    connect_refused_marker = (
        f"{thrd_prefix} localhost:{non_existent_broker_port}/bootstrap: Connect to"
    )
    matching_count = instance.query_with_retry(
        f"""
        SELECT count()
        FROM system.kafka_consumers
        ARRAY JOIN exceptions
        WHERE table = 'foo_exceptions'
          AND (startsWith(exceptions.text, '{broker_down_marker}')
               OR startsWith(exceptions.text, '{connect_refused_marker}'))
        """,
        check_callback=lambda res: int(res.strip()) >= 1,
        retry_count=max_retries,
        sleep_time=1,
    )
    if int(matching_count.strip()) < 1:
        # Surface the full exceptions array on failure to make debugging easier.
        all_exceptions = instance.query(
            "SELECT exceptions.text FROM system.kafka_consumers ARRAY JOIN exceptions WHERE table = 'foo_exceptions'"
        )
        logging.debug(f"system.kafka_consumers content: {all_exceptions}")
        raise AssertionError(
            f"Expected at least one entry in system.kafka_consumers.exceptions starting with "
            f"either {broker_down_marker!r} or {connect_refused_marker!r}, "
            f"but none was found. Captured exceptions:\n{all_exceptions}"
        )

    instance.query("DROP TABLE foo_exceptions")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
