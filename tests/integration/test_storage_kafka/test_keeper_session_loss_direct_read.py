"""A direct read from an inactive `StorageKafka2` must fail with a regular exception.

When the Keeper session expires, `partialShutdown` marks the table inactive until it is
reactivated. A direct read (`Kafka2Source::generateImpl`) reopening its Keeper then hits
`StorageKafka2::assertActive`, which must throw `ABORTED` instead of `LOGICAL_ERROR`
(a logical error aborts the server in debug and sanitizer builds).
"""

import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
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
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    k.clean_test_database_and_topics(instance, cluster)
    yield


def test_direct_read_from_inactive_table_is_not_a_logical_error(kafka_cluster):
    suffix = k.random_string(6)
    kafka_table = f"kafka_inactive_{suffix}"
    topic_name = f"inactive_direct_read_{suffix}"

    admin_client = k.get_admin_client(kafka_cluster)

    with k.kafka_topic(admin_client, topic_name):
        instance.query(
            k.generate_new_create_table_query(
                kafka_table,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
            )
        )
        instance.wait_for_log_line(f"{kafka_table}.*Table activated successfully")

        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(instance)
            # The activation task notices the expired session (its check period is one
            # minute), runs `partialShutdown` and, since Keeper is unreachable, fails to
            # reactivate: from this line on the table stays inactive while the network
            # partition holds.
            instance.wait_for_log_line(
                f"{kafka_table}.*Failed to establish a new ZK connection. Will try again",
                timeout=180,
            )

            error = instance.query_and_get_error(
                f"SELECT * FROM test.{kafka_table} LIMIT 1"
            )
            logging.debug("Direct read from an inactive table failed with: %s", error)
            assert "Table is not active" in error
            assert "Code: 236" in error  # ABORTED
            assert "Logical error" not in error

        # The server must survive (in debug builds a logical error would abort it).
        assert instance.query("SELECT 1").strip() == "1"

        instance.query(f"DROP TABLE test.{kafka_table} SYNC")
