"""Test SYSTEM STOP/PAUSE/CANCEL/REFRESH and ALL BACKGROUND controls on a NATS table."""

# NATS core delivery has no backlog: messages published while there is no subscriber are
# dropped. The STOP/PAUSE tests therefore assert that consumption resumes for freshly
# published messages, not that a backlog is drained.

import asyncio
import json
import time

import pytest

from helpers.cluster import ClickHouseCluster, nats_connect_ssl
from helpers.config_cluster import nats_user, nats_pass

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/nats.xml"],
    with_nats=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def nats_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    instance.query("DROP DATABASE IF EXISTS test SYNC; CREATE DATABASE test;")
    yield


def nats_publish(nats_cluster, subject, start, count):
    async def run():
        nc = await nats_connect_ssl(nats_cluster)
        for i in range(start, start + count):
            await nc.publish(subject, json.dumps({"key": i, "value": i}).encode())
        await nc.flush()
        await nc.close()

    asyncio.run(run())


def setup_consuming_table(table, subject):
    instance.query(
        f"""
        CREATE TABLE test.{table} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{subject}',
                     nats_format = 'JSONEachRow',
                     nats_secure = 1,
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}';

        CREATE TABLE test.{table}_dst (key UInt64, value UInt64)
            ENGINE = MergeTree ORDER BY key;

        CREATE MATERIALIZED VIEW test.{table}_mv TO test.{table}_dst AS
            SELECT key, value FROM test.{table};
        """
    )
    instance.wait_for_log_line("Started streaming to 1 attached views")


def wait_dst_count_at_least(table, expected):
    instance.query_with_retry(
        f"SELECT count() FROM test.{table}_dst",
        check_callback=lambda res: int(res) >= expected,
        retry_count=120,
        sleep_time=0.5,
    )


def assert_dst_count_stable(table, expected, seconds=10):
    """The consumer is expected to be stopped, so the row count must not grow."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        assert int(instance.query(f"SELECT count() FROM test.{table}_dst")) == expected
        time.sleep(1)


def test_system_stop_start_consuming(nats_cluster):
    table = "nats_stop"
    subject = "stop_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # STOP halts consumption: messages published with no subscriber are dropped.
    instance.query(f"SYSTEM STOP test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    # START resumes consumption: freshly published messages are delivered again.
    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line("Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_pause_start_consuming(nats_cluster):
    table = "nats_pause"
    subject = "pause_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    instance.query(f"SYSTEM PAUSE test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    instance.query(f"SYSTEM START test.{table}")
    instance.wait_for_log_line("Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_cancel_consuming(nats_cluster):
    table = "nats_cancel"
    subject = "cancel_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # CANCEL interrupts only the current poll; the subscription stays active, so
    # subsequently published messages are still consumed.
    instance.query(f"SYSTEM CANCEL test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_system_refresh_consuming(nats_cluster):
    table = "nats_refresh"
    subject = "refresh_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # REFRESH kicks off a poll out of order; streaming continues normally.
    instance.query(f"SYSTEM REFRESH test.{table}")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_system_stop_all_background(nats_cluster):
    table = "nats_allbg"
    subject = "allbg_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    instance.query("SYSTEM STOP ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    # START ALL BACKGROUND resumes every streaming table.
    instance.query("SYSTEM START ALL BACKGROUND")
    instance.wait_for_log_line("Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_pause_all_background(nats_cluster):
    table = "nats_pauseall"
    subject = "pauseall_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # PAUSE ALL BACKGROUND halts consumption for every streaming table.
    instance.query("SYSTEM PAUSE ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    assert_dst_count_stable(table, 10)

    instance.query("SYSTEM START ALL BACKGROUND")
    instance.wait_for_log_line("Started streaming to 1 attached views")
    nats_publish(nats_cluster, subject, 20, 10)
    wait_dst_count_at_least(table, 20)


def test_system_cancel_all_background(nats_cluster):
    table = "nats_cancelall"
    subject = "cancelall_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # CANCEL ALL BACKGROUND interrupts the current poll but keeps the subscription, so
    # subsequently published messages are still consumed.
    instance.query("SYSTEM CANCEL ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_system_refresh_all_background(nats_cluster):
    table = "nats_refreshall"
    subject = "refreshall_subject"
    setup_consuming_table(table, subject)

    nats_publish(nats_cluster, subject, 0, 10)
    wait_dst_count_at_least(table, 10)

    # REFRESH ALL BACKGROUND kicks off a poll; streaming continues normally.
    instance.query("SYSTEM REFRESH ALL BACKGROUND")
    nats_publish(nats_cluster, subject, 10, 10)
    wait_dst_count_at_least(table, 20)


def test_system_stop_requires_grant(nats_cluster):
    table = "nats_grant"
    subject = "grant_subject"
    setup_consuming_table(table, subject)
    user = f"user_{table}"
    instance.query(f"DROP USER IF EXISTS {user}; CREATE USER {user}")

    # A user without the required SYSTEM privilege cannot control the table.
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        assert "ACCESS_DENIED" in instance.query_and_get_error(
            f"SYSTEM {verb} test.{table}", user=user
        )

    # Once granted, every verb succeeds.
    instance.query(f"GRANT SYSTEM ON *.* TO {user}")
    for verb in ["STOP", "START", "PAUSE", "CANCEL", "REFRESH"]:
        instance.query(f"SYSTEM {verb} test.{table}", user=user)

    instance.query(f"DROP USER {user}")
