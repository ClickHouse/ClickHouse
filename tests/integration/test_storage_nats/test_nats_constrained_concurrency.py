import asyncio
import json
import logging
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import nats_user, nats_pass

from . import common as nats_helpers

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/nats.xml",
        "configs/macros.xml",
        "configs/named_collection.xml",
        "configs/concurrency_limit_1.xml",
    ],
    user_configs=["configs/users.xml"],
    with_nats=True,
    clickhouse_path_dir="clickhouse_path",
)


@pytest.fixture(scope="module")
def nats_cluster():
    try:
        cluster.start()
        logging.debug("nats_id is {}".format(instance.cluster.nats_docker_id))
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def nats_setup_teardown(nats_cluster):
    logging.debug("NATS is available - running test")

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    yield  # run test

    instance.query("DROP DATABASE test")


def test_nats_wait_for_flush_interval_reads_all_consumers_constrained_concurrency(nats_cluster):
    table_name = "test_serial_consumers_constrained"
    subject = "serial_consumers_constrained"
    messages_count = 300

    assert (
        instance.query(
            "SELECT value FROM system.server_settings WHERE name = 'concurrent_threads_soft_limit_num'"
        ).strip()
        == "1"
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.{table_name};
        DROP TABLE IF EXISTS test.dst;
        DROP VIEW IF EXISTS test.mv;
        CREATE TABLE test.{table_name} (key UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_format = 'JSONEachRow',
                     nats_num_consumers = 4,
                     nats_flush_interval_ms = 1000,
                     nats_wait_for_flush_interval = 1,
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';
        CREATE TABLE test.dst (key UInt64) ENGINE = MergeTree ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, f"test.{table_name}")

    instance.query(
        f"""
        CREATE MATERIALIZED VIEW test.mv TO test.dst AS SELECT key FROM test.{table_name};
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, f"test.{table_name}")

    async def publish():
        nc = await nats_helpers.nats_connect_ssl(nats_cluster)
        for i in range(messages_count):
            await nc.publish(subject, json.dumps({"key": i}).encode())
            await nc.flush()
            await asyncio.sleep(0.01)
        await nc.close()

    asyncio.run(publish())

    result = instance.query_with_retry(
        "SELECT count(), uniqExact(key), min(key), max(key) FROM test.dst",
        check_callback=lambda res: res.strip() == f"{messages_count}\t{messages_count}\t0\t{messages_count - 1}",
        retry_count=60,
        sleep_time=1,
    )
    assert result.strip() == f"{messages_count}\t{messages_count}\t0\t{messages_count - 1}"
