from helpers.kafka.common_direct import *
from helpers.kafka.common_direct import _VarintBytes
import helpers.kafka.common as k
import os

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_minio=True,  # needed for resolver image only
    with_zookeeper=True,  # needed for Kafka2
    env_variables={
        "AWS_EC2_METADATA_SERVICE_ENDPOINT": "http://resolver:8080",
    },
    clickhouse_path_dir="clickhouse_path",
)


# Runs custom python-based S3 endpoint.
def run_endpoint(cluster):
    logging.info("Starting custommm2 S3 endpoint")
    # container_id = cluster.get_container_id("instance")
    container_id = cluster.get_container_id("resolver")
    current_dir = os.path.dirname(__file__)
    cluster.copy_file_to_container(
        container_id,
        os.path.join(current_dir, "s3_endpoint", "endpoint.py"),
        "endpoint.py",
    )
    logging.info("Before executing")
    ret = cluster.exec_in_container(
        container_id, ["python", "endpoint.py"], detach=True
    )
    logging.info(f"Executed in container with output {ret}")

    # Wait for S3 endpoint start
    num_attempts = 100
    for attempt in range(num_attempts):
        logging.info(f"attempt {attempt}")
        ping_response = cluster.exec_in_container(
            container_id,
            ["curl", "-s", "http://localhost:8080/ping"],
            nothrow=True,
        )
        if ping_response != "OK":
            if attempt == num_attempts - 1:
                assert ping_response == "OK", 'Expected "OK", but got "{}"'.format(
                    ping_response
                )
            else:
                time.sleep(1)
        else:
            break
    logging.info("S3 endpoint started")


# Fixtures
@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        run_endpoint(cluster)
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
        (k.generate_old_create_table_query),
        (k.generate_new_create_table_query),
    ],
)
def test_kafka_zone_awareness(kafka_cluster, create_query_generator):
    suffix = k.random_string(6)
    kafka_table = f"kafka_{suffix}"
    topname = "zone_awareness"
    instance.rotate_logs()

    # Check that matview does respect Kafka SETTINGS
    k.kafka_produce(
        kafka_cluster,
        topname,
        [
            '{"t": 123, "e": {"x": "woof"} }',
            '{"t": 123, "e": {"x": "woof"} }',
            '{"t": 124, "e": {"x": "test"} }',
        ],
    )

    instance.query(
        f"""
        CREATE TABLE test.persistent_{kafka_table} (
            time UInt64,
            some_string String
        )
        ENGINE = MergeTree()
        ORDER BY time;

        {create_query_generator(kafka_table, "t UInt64, `e.x` String", brokers='kafka1:19092', topic_list=topname, consumer_group=topname, format='JSONEachRow', settings={'kafka_autodetect_client_rack':'MSK', 'input_format_import_nested_json':1})};

        CREATE MATERIALIZED VIEW test.persistent_{kafka_table}_mv TO test.persistent_{kafka_table} AS
        SELECT
            `t` AS `time`,
            `e.x` AS `some_string`
        FROM test.{kafka_table};
    """
    )

    while int(instance.query(f"SELECT count() FROM test.persistent_{kafka_table}")) < 3:
        time.sleep(1)

    result = instance.query(
        f"SELECT * FROM test.persistent_{kafka_table} ORDER BY time"
    )

    instance.query(
        f"""
        DROP TABLE test.persistent_{kafka_table};
        DROP TABLE test.persistent_{kafka_table}_mv;
    """
    )

    expected = """\
123	woof
123	woof
124	test
"""
    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log("client.rack set to euc1-az2")
