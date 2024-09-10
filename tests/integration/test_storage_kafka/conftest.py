from typing import Iterator
import pytest
import logging
import time

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from test_storage_kafka.kafka_tests_utils import get_admin_client

pytestmark = pytest.mark.skip

KAFKA_TOPIC_OLD = "old_t"
KAFKA_CONSUMER_GROUP_OLD = "old_cg"
KAFKA_TOPIC_NEW = "new_t"
KAFKA_CONSUMER_GROUP_NEW = "new_cg"

# We have to initialize these lazily, otherwise WORKER_FREE_PORTS is not populated making the PortPoolManager fail.
# It would be nice to initialize these eagerly, but the fixtures has to be in conftest.py to apply them automatically,
# so they "cannot be forgotten". We could put these variables and the gist of the fixture implementation into a separate
# file, but I think splitting these has more disadvantages than advantages of eager initialization.
conftest_cluster = None
conftest_instance = None


def init_cluster_and_instance():
    global conftest_cluster
    global conftest_instance
    if conftest_cluster is not None:
        return

    conftest_cluster = ClickHouseCluster(__file__)
    conftest_instance = conftest_cluster.add_instance(
        "instance",
        main_configs=["configs/kafka.xml", "configs/named_collection.xml"],
        user_configs=["configs/users.xml"],
        with_kafka=True,
        with_zookeeper=True,  # For Replicated Table
        macros={
            "kafka_broker": "kafka1",
            "kafka_topic_old": KAFKA_TOPIC_OLD,
            "kafka_group_name_old": KAFKA_CONSUMER_GROUP_OLD,
            "kafka_topic_new": KAFKA_TOPIC_NEW,
            "kafka_group_name_new": KAFKA_CONSUMER_GROUP_NEW,
            "kafka_client_id": "instance",
            "kafka_format_json_each_row": "JSONEachRow",
        },
        clickhouse_path_dir="clickhouse_path",
    )


@pytest.fixture(scope="module")
def kafka_cluster() -> "Iterator[ClickHouseCluster]":
    try:
        init_cluster_and_instance()
        conftest_cluster.start()
        kafka_id = conftest_cluster.kafka_docker_id
        logging.info(f"kafka_id is {kafka_id}")
        yield conftest_cluster
    finally:
        conftest_cluster.shutdown()


# kafka_cluster is requested here to ensure the cluster is initialized before we yield the instance
@pytest.fixture()
def instance(kafka_cluster) -> "Iterator[ClickHouseInstance]":
    yield conftest_instance


@pytest.fixture(autouse=True)
def kafka_setup_teardown(kafka_cluster, instance):
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    admin_client = get_admin_client(kafka_cluster)

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
