"""Long running tests, longer than 30 seconds"""

from helpers.kafka.common_direct import *
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,  # For Replicated Table
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


# Fixtures
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

    def get_topics_to_delete():
        return [t for t in admin_client.list_topics() if not t.startswith("_")]

    topics = get_topics_to_delete()
    logging.debug(f"Deleting topics: {topics}")
    result = admin_client.delete_topics(topics)
    for topic, error in result.topic_error_codes:
        if error != 0:
            logging.warning(f"Received error {error} while deleting topic {topic}")
        else:
            logging.info(f"Deleted topic {topic}")

    retries = 0
    topics = get_topics_to_delete()
    while len(topics) != 0:
        logging.info(f"Existing topics: {topics}")
        if retries >= 5:
            raise Exception(f"Failed to delete topics {topics}")
        retries += 1
        time.sleep(0.5)
    yield  # run test


# Tests


@pytest.mark.parametrize(
    "create_query_generator",
    [
        k.generate_old_create_table_query,
        k.generate_new_create_table_query,
    ],
)
def test_kafka_consumer_failover(kafka_cluster, create_query_generator):
    topic_name = "kafka_consumer_failover" + k.get_topic_postfix(create_query_generator)

    with k.kafka_topic(k.get_admin_client(kafka_cluster), topic_name, num_partitions=2):
        consumer_group = f"{topic_name}_group"
        create_queries = []
        for counter in range(3):
            create_queries.append(
                create_query_generator(
                    f"kafka{counter+1}",
                    "key UInt64, value UInt64",
                    topic_list=topic_name,
                    consumer_group=consumer_group,
                    settings={
                        "kafka_max_block_size": 1,
                        "kafka_poll_timeout_ms": 200,
                    },
                )
            )

        instance.query(
            f"""
            {create_queries[0]};
            {create_queries[1]};
            {create_queries[2]};

            CREATE TABLE test.destination (
                key UInt64,
                value UInt64,
                _consumed_by LowCardinality(String)
            )
            ENGINE = MergeTree()
            ORDER BY key;

            CREATE MATERIALIZED VIEW test.kafka1_mv TO test.destination AS
            SELECT key, value, 'kafka1' as _consumed_by
            FROM test.kafka1;

            CREATE MATERIALIZED VIEW test.kafka2_mv TO test.destination AS
            SELECT key, value, 'kafka2' as _consumed_by
            FROM test.kafka2;

            CREATE MATERIALIZED VIEW test.kafka3_mv TO test.destination AS
            SELECT key, value, 'kafka3' as _consumed_by
            FROM test.kafka3;
            """
        )

        producer = KafkaProducer(
            bootstrap_servers="localhost:{}".format(cluster.kafka_port),
            value_serializer=k.producer_serializer,
            key_serializer=k.producer_serializer,
        )

        ## all 3 attached, 2 working
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 1, "value": 1}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 1, "value": 1}),
            partition=1,
        )
        producer.flush()

        count_query = "SELECT count() FROM test.destination"
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > 0
        )

        ## 2 attached, 2 working
        instance.query("DETACH TABLE test.kafka1")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 2, "value": 2}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 2, "value": 2}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 1 attached, 1 working
        instance.query("DETACH TABLE test.kafka2")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 3, "value": 3}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 3, "value": 3}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, 2 working
        instance.query("ATTACH TABLE test.kafka1")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 4, "value": 4}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 4, "value": 4}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 1 attached, 1 working
        instance.query("DETACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 5, "value": 5}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 5, "value": 5}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, 2 working
        instance.query("ATTACH TABLE test.kafka2")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 6, "value": 6}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 6, "value": 6}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 3 attached, 2 working
        instance.query("ATTACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 7, "value": 7}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 7, "value": 7}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, same 2 working
        instance.query("DETACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 8, "value": 8}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 8, "value": 8}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )
