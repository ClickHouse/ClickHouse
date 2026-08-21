import time

import pytest
from kafka import KafkaAdminClient

from helpers.kafka.common_direct import *
from helpers.cluster import ClickHouseCluster
import helpers.kafka.common as k

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/kafka.xml"],
    with_kafka=True,
    with_zookeeper=True,
    macros={
        "kafka_broker": "kafka1",
        "kafka_format_json_each_row": "JSONEachRow",
    },
)


@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
def test_missing_mv_target(kafka_cluster, create_query_generator):
    admin = k.get_admin_client(kafka_cluster)
    topic = "mv_target_missing" + k.get_topic_postfix(create_query_generator)
    flush_interval_seconds = 1

    # we don't bother creating this topic because we never start streaming
    topic_other = "mv_target_missing_other" + k.get_topic_postfix(
        create_query_generator
    )

    with k.kafka_topic(admin, topic):

        settings = {
            "kafka_row_delimiter": "\n",
            "format_csv_delimiter": "|",
            "kafka_flush_interval_ms": flush_interval_seconds * 1000,
        }
        if create_query_generator == k.generate_old_create_table_query:
            settings["kafka_commit_on_select"] = 1

        create_query = create_query_generator(
            "kafkamiss",
            "a UInt64, b String",
            topic_list=topic,
            consumer_group=topic,
            format="CSV",
            settings=settings,
        )

        create_other_query = create_query_generator(
            "kafkamissother",
            "a UInt64, b String",
            topic_list=topic_other,
            consumer_group=topic,
            format="CSV",
            settings=settings,
        )

        kafka_mv_not_ready_before_str = instance.query(
            "SELECT value FROM system.events WHERE event='KafkaMVNotReady'"
        )
        kafka_mv_not_ready_before = (
            int(kafka_mv_not_ready_before_str)
            if kafka_mv_not_ready_before_str != ""
            else 0
        )

        instance.query(
            f"""
            SET allow_materialized_view_with_bad_select = true;
            DROP TABLE IF EXISTS test.kafka;
            DROP TABLE IF EXISTS test.target1;
            DROP TABLE IF EXISTS test.target2;
            DROP TABLE IF EXISTS test.mv1;
            DROP TABLE IF EXISTS test.mv2;

            DROP DATABASE IF EXISTS test;
            CREATE DATABASE test;

            {create_query};
            {create_other_query};

            CREATE MATERIALIZED VIEW test.mv1 TO test.target1 AS SELECT * FROM test.kafkamiss;
            CREATE MATERIALIZED VIEW test.mv2 TO test.target2 AS SELECT * FROM test.kafkamiss;
            """
        )

        kafka_mv_not_ready_after = int(
            instance.query_with_retry(
                "SELECT value FROM system.events WHERE event='KafkaMVNotReady'",
                check_callback=lambda x: int(x) > kafka_mv_not_ready_before,
            )
        )

        assert (
            kafka_mv_not_ready_after > kafka_mv_not_ready_before
            and kafka_mv_not_ready_after < kafka_mv_not_ready_before + 100
        )

        skc = instance.query(
            "SELECT dependencies, missing_dependencies FROM system.kafka_consumers WHERE table in ('kafkamiss', 'kafkamissother') ORDER BY table FORMAT Vertical"
        )
        assert (
            skc
            == """Row 1:
──────
dependencies:         [['test.mv2'],['test.mv1']]
missing_dependencies: [['test.mv2'],['test.mv1']]

Row 2:
──────
dependencies:         []
missing_dependencies: []
"""
        )

        instance.query(
            """
            SET allow_materialized_view_with_bad_select = true;
            CREATE TABLE test.target2 (a UInt64, b String) ENGINE = MergeTree() ORDER BY a;
            CREATE MATERIALIZED VIEW test.mvother TO test.targetother AS SELECT * FROM test.kafkamissother;
            """
        )

        skc = instance.query(
            "SELECT dependencies, missing_dependencies FROM system.kafka_consumers WHERE table in ('kafkamiss', 'kafkamissother') ORDER BY table FORMAT Vertical"
        )
        assert (
            skc
            == """Row 1:
──────
dependencies:         [['test.mv2','test.target2'],['test.mv1']]
missing_dependencies: [['test.mv1']]

Row 2:
──────
dependencies:         [['test.mvother']]
missing_dependencies: [['test.mvother']]
"""
        )

        instance.query(
            "CREATE TABLE test.target1 (a UInt64, b String) ENGINE = MergeTree() ORDER BY a"
        )

        skc = instance.query(
            "SELECT dependencies, missing_dependencies FROM system.kafka_consumers WHERE table in ('kafkamiss', 'kafkamissother') ORDER BY table FORMAT Vertical"
        )
        assert (
            skc
            == """Row 1:
──────
dependencies:         [['test.mv2','test.target2'],['test.mv1','test.target1']]
missing_dependencies: []

Row 2:
──────
dependencies:         [['test.mvother']]
missing_dependencies: [['test.mvother']]
"""
        )

        # make sure the kafka table engine picked up the views, because it happens at the beginning of the streaming loop
        time.sleep(flush_interval_seconds * 1.2)
        k.kafka_produce(kafka_cluster, topic, ["1|foo", "2|bar"])

        assert (
            instance.query_with_retry(
                "SELECT count() FROM test.target2",
                retry_count=100,
                check_callback=lambda x: int(x) == 2,
            ).strip()
            == "2"
        )
        assert (
            instance.query_with_retry(
                "SELECT count() FROM test.target1",
                retry_count=100,
                check_callback=lambda x: int(x) == 2,
            ).strip()
            == "2"
        )

        instance.query(
            """
            DROP TABLE test.mv1;
            DROP TABLE test.mv2;
            DROP TABLE test.mvother;
            DROP TABLE test.target1;
            DROP TABLE test.target2;
            DROP TABLE test.kafkamiss;
            DROP TABLE test.kafkamissother;
            DROP DATABASE test;
            """
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [k.generate_old_create_table_query, k.generate_new_create_table_query],
)
# kafka ->  mv1 -> null -> mv2 -> nonexistent_merge_tree
def test_missing_mv_transitive_target(kafka_cluster, create_query_generator):
    admin = k.get_admin_client(kafka_cluster)
    topic = "mv_transitive_target_missing" + k.get_topic_postfix(create_query_generator)
    flush_interval_seconds = 1

    with k.kafka_topic(admin, topic):

        settings = {
            "kafka_row_delimiter": "\n",
            "format_csv_delimiter": "|",
            "kafka_flush_interval_ms": flush_interval_seconds * 1000,
        }
        if create_query_generator == k.generate_old_create_table_query:
            settings["kafka_commit_on_select"] = 1

        create_query = create_query_generator(
            "tkafkamiss",
            "a UInt64, b String",
            topic_list=topic,
            consumer_group=topic,
            format="CSV",
            settings=settings,
        )

        instance.query(
            f"""
            SET allow_materialized_view_with_bad_select = true;
            DROP TABLE IF EXISTS test.tkafka;
            DROP TABLE IF EXISTS test.target1;
            DROP TABLE IF EXISTS test.target2;
            DROP TABLE IF EXISTS test.mv1;
            DROP TABLE IF EXISTS test.mv2;

            DROP DATABASE IF EXISTS test;
            CREATE DATABASE test;

            {create_query};

            CREATE TABLE test.target1 (a UInt64, b String) ENGINE = Null;
            CREATE MATERIALIZED VIEW test.mv1 TO test.target1 AS SELECT * FROM test.tkafkamiss;
            CREATE MATERIALIZED VIEW test.mv2 TO test.target2 AS SELECT * FROM test.target1;
            """
        )

        skc = instance.query(
            "SELECT dependencies, missing_dependencies FROM system.kafka_consumers WHERE table='tkafkamiss' FORMAT Vertical"
        )
        assert (
            skc
            == """Row 1:
──────
dependencies:         [['test.mv1','test.target1','test.mv2']]
missing_dependencies: [['test.mv1','test.target1','test.mv2']]
"""
        )

        instance.query(
            "CREATE TABLE test.target2 (a UInt64, b String) ENGINE = MergeTree() ORDER BY a"
        )

        skc = instance.query(
            "SELECT dependencies, missing_dependencies FROM system.kafka_consumers WHERE table='tkafkamiss' FORMAT Vertical"
        )
        assert (
            skc
            == """Row 1:
──────
dependencies:         [['test.mv1','test.target1','test.mv2','test.target2']]
missing_dependencies: []
"""
        )

        # The old table engine will wait for assignment up to MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS after the consumer is
        # created and that it won't be interrupted by flush interval (which make sense, we want to get assignment as
        # soon as possible, because getting stuck in a rebalance loop is expensive for all consumers in the consumer
        # group, not just for one). Therefore waiting the flush interval seconds will only ensure the new target is
        # picked up the engine after the assignment is done.

        # Here we have to make sure the kafka consumer got assignment, because it can wait more than the flush interval,
        # so let's use `system.kafka_consumers` to ensure that.
        assigned_partition_count = instance.query_with_retry(
            "SELECT count() FROM system.kafka_consumers WHERE table='tkafkamiss' AND length(assignments.partition_id) > 0",
            retry_count=100,
            check_callback=lambda x: int(x) > 0,
        )
        assert (
            int(assigned_partition_count) > 0
        ), "Kafka consumer did not get assignment in time"

        # After making sure we have an assignment, let's wait the flush interval to make sure the kafka table engine
        # picked up the views, because it happens at the beginning of the streaming loop.
        time.sleep(flush_interval_seconds * 1.2)

        k.kafka_produce(kafka_cluster, topic, ["1|foo", "2|bar"])

        assert (
            instance.query_with_retry(
                "SELECT count() FROM test.target2",
                retry_count=100,
                check_callback=lambda x: int(x) == 2,
            ).strip()
            == "2"
        )
        instance.query(
            """
            DROP TABLE test.mv1;
            DROP TABLE test.mv2;
            DROP TABLE test.target1;
            DROP TABLE test.target2;
            DROP TABLE test.tkafkamiss;
            DROP DATABASE test;
            """
        )
