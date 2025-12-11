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

    # we don't bother creating this topic because we never start streaming
    topic_other = "mv_target_missing_other" + k.get_topic_postfix(
        create_query_generator
    )

    with k.kafka_topic(admin, topic):

        settings = {
            "kafka_row_delimiter": "\n",
            "format_csv_delimiter": "|",
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

        k.kafka_produce(kafka_cluster, topic, ["1|foo", "2|bar"])

        assert (
            instance.query_with_retry(
                "SELECT count() FROM test.target2",
                check_callback=lambda x: int(x) == 2,
            ).strip()
            == "2"
        )
        assert (
            instance.query_with_retry(
                "SELECT count() FROM test.target1",
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

    with k.kafka_topic(admin, topic):

        settings = {
            "kafka_row_delimiter": "\n",
            "format_csv_delimiter": "|",
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

        k.kafka_produce(kafka_cluster, topic, ["1|foo", "2|bar"])

        assert (
            instance.query_with_retry(
                "SELECT count() FROM test.target2",
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
