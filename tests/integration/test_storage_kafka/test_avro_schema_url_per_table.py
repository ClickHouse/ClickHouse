import logging
import time

import pytest

import helpers.kafka.common as k
from helpers.kafka.common_direct import *

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka_and_keeper.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_client_id": "instance",
    },
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
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_avro_schema_url_per_table(kafka_cluster, create_query_generator):
    schema_registry_client = CachedSchemaRegistryClient(
        {"url": f"http://localhost:{kafka_cluster.schema_registry_port}"}
    )
    topic_name_prefix = "test_avro_schema_url_"
    topic_name_postfix = k.get_topic_postfix(create_query_generator)
    topic1_name = f"{topic_name_prefix}_1_{topic_name_postfix}"
    topic2_name = f"{topic_name_prefix}_2_{topic_name_postfix}"

    k.kafka_produce(
        kafka_cluster,
        topic1_name,
        [
            k.avro_confluent_message(
                schema_registry_client,
                {
                    "id": 1,
                    "name": "foo",
                },
            ),
            k.avro_confluent_message(
                schema_registry_client,
                {
                    "id": 2,
                    "name": "bar",
                },
            ),
            k.avro_confluent_message(
                schema_registry_client,
                {
                    "id": 3,
                    "name": "baz",
                },
            ),
        ],
    )
    k.kafka_produce(
        kafka_cluster,
        topic2_name,
        [
            k.avro_confluent_message(
                schema_registry_client,
                {
                    "id": 1,
                    "blockNo": 1,
                    "val1": "foo",
                },
            ),
            k.avro_confluent_message(
                schema_registry_client,
                {
                    "id": 2,
                    "blockNo": 2,
                    "val1": "bar",
                },
            ),
            k.avro_confluent_message(
                schema_registry_client,
                {
                    "id": 3,
                    "blockNo": 3,
                    "val1": "baz",
                },
            ),
        ],
    )

    kafka_settings = {
        "kafka_format_avro_schema_registry_url": f"http://{kafka_cluster.schema_registry_host}:{kafka_cluster.schema_registry_port}",
    }
    create_query1 = create_query_generator(
        "kafka_1",
        "id Int64, name String",
        format="AvroConfluent",
        topic_list=topic1_name,
        consumer_group=f"{topic1_name}_group",
        settings=kafka_settings,
    )
    create_query2 = create_query_generator(
        "kafka_2",
        "id Int64, blockNo UInt16, val1 String",
        format="AvroConfluent",
        topic_list=topic2_name,
        consumer_group=f"{topic2_name}_group",
        settings=kafka_settings,
    )

    query = """
    DROP TABLE IF EXISTS test.kafka_{i};

    {create_query};

    DROP TABLE IF EXISTS test.kafka_{i}_mv;

    CREATE MATERIALIZED VIEW test.kafka_{i}_mv ENGINE=MergeTree ORDER BY tuple() AS
        SELECT *, _topic, _partition, _offset FROM test.kafka_{i};
    """

    instance.query(query.format(i=1, create_query=create_query1))
    instance.query(query.format(i=2, create_query=create_query2))

    expected = [
        f"""\
1	foo	{topic1_name}	0	0
2	bar	{topic1_name}	0	1
3	baz	{topic1_name}	0	2
""",
        f"""\
1	1	foo	{topic2_name}	0	0
2	2	bar	{topic2_name}	0	1
3	3	baz	{topic2_name}	0	2
""",
    ]

    for i in [1, 2]:
        expected_result = expected[i - 1]
        expected_rows_count = expected_result.count("\n")
        result_checker = lambda res: res.count("\n") == expected_rows_count
        res = instance.query_with_retry(
            f"SELECT * FROM test.kafka_{i}_mv;",
            retry_count=30,
            sleep_time=1,
            check_callback=result_checker,
        )
        assert result_checker(res)

        result = instance.query_with_retry(
            f"SELECT * FROM test.kafka_{i}_mv;",
            check_callback=lambda x: x.count("\n") == expected_rows_count,
        )
        assert TSV(result) == TSV(expected_result)
