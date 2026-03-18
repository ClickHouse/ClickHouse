"""
Regression test for StorageNATS startup data-loss race (follow-up to PR #99700).

PR #99700 fixes the startup race for Kafka, FileLog, RabbitMQ, and ObjectStorageQueue
by centralizing dependency checks in DatabaseCatalog::getReadyDependentViews, which
returns empty while isServerCompletelyStarted() is false.

StorageNATS::checkDependencies (StorageNATS.cpp:602) still uses the old
getDependentViews pattern without the startup guard. This means NATS consumers
can process batches before all MV dependencies are loaded after a server restart,
causing permanent data loss for views not yet registered in DatabaseCatalog.

This test catches the bug by:
1. Creating a NATS source table with TWO materialized views (different target tables).
2. Publishing a baseline batch and verifying both MVs receive all rows.
3. Restarting ClickHouse and immediately publishing a second batch while the server
   is still loading tables.
4. Verifying both target tables end up with the same total row count.

With the bug (getDependentViews, no startup guard):
  - NATS consumers start processing as soon as either MV is registered.
  - If MV1 loads before MV2, messages arrive only in target1 → counts diverge.

With the fix (getReadyDependentViews, startup guard):
  - NATS consumers do not process anything until isServerCompletelyStarted() is true.
  - Both MVs are guaranteed to be loaded → counts are always equal.
"""

import asyncio
import time

import nats as nats_client
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import nats_pass, nats_user

SUBJECT = "test_startup_race"
MSG_COUNT = 50
FLUSH_INTERVAL_MS = 200

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/nats.xml"],
    with_nats=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_tables(ch):
    ch.query(
        f"""
        DROP TABLE IF EXISTS nats_source;
        DROP TABLE IF EXISTS nats_target1;
        DROP TABLE IF EXISTS nats_target2;
        DROP TABLE IF EXISTS nats_mv1;
        DROP TABLE IF EXISTS nats_mv2;

        CREATE TABLE nats_source (val UInt64)
            ENGINE = NATS
            SETTINGS
                nats_url = 'nats1:4444',
                nats_subjects = '{SUBJECT}',
                nats_format = 'JSONEachRow',
                nats_secure = true,
                nats_username = '{nats_user}',
                nats_password = '{nats_pass}',
                nats_flush_interval_ms = {FLUSH_INTERVAL_MS},
                nats_num_consumers = 1;

        CREATE TABLE nats_target1 (val UInt64)
            ENGINE = MergeTree ORDER BY val;

        CREATE TABLE nats_target2 (val UInt64)
            ENGINE = MergeTree ORDER BY val;

        CREATE MATERIALIZED VIEW nats_mv1 TO nats_target1
            AS SELECT val FROM nats_source;

        CREATE MATERIALIZED VIEW nats_mv2 TO nats_target2
            AS SELECT val FROM nats_source;
        """
    )


async def publish_messages(nats_port, count, start_val=0):
    ssl_ctx = cluster.nats_ssl_context
    import ssl

    if not ssl_ctx:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    nc = await nats_client.connect(
        f"tls://localhost:{nats_port}",
        user=nats_user,
        password=nats_pass,
        tls=ssl_ctx,
    )
    try:
        for i in range(count):
            msg = f'{{"val":{start_val + i}}}'.encode()
            await nc.publish(SUBJECT, msg)
        await nc.flush()
    finally:
        await nc.close()


def wait_for_count(ch, table, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = int(ch.query(f"SELECT count() FROM {table}").strip())
        if got >= expected:
            return got
        time.sleep(0.5)
    return int(ch.query(f"SELECT count() FROM {table}").strip())


def test_both_mvs_receive_all_messages(started_cluster):
    """Both MVs must receive every message — basic sanity check."""
    create_tables(instance)

    asyncio.run(publish_messages(started_cluster.nats_port, MSG_COUNT, start_val=0))

    count1 = wait_for_count(instance, "nats_target1", MSG_COUNT)
    count2 = wait_for_count(instance, "nats_target2", MSG_COUNT)

    assert count1 == MSG_COUNT, f"target1 got {count1}, expected {MSG_COUNT}"
    assert count2 == MSG_COUNT, f"target2 got {count2}, expected {MSG_COUNT}"


def test_no_data_loss_after_restart(started_cluster):
    """
    After a server restart, both MVs must receive the same number of messages.

    Without the fix: NATS may start consuming before all MVs are registered,
    so one MV misses messages published in the startup window → counts diverge.
    With the fix: NATS waits for isServerCompletelyStarted() before consuming.
    """
    create_tables(instance)

    # Baseline: both MVs receive MSG_COUNT messages before restart
    asyncio.run(publish_messages(started_cluster.nats_port, MSG_COUNT, start_val=0))
    wait_for_count(instance, "nats_target1", MSG_COUNT)
    wait_for_count(instance, "nats_target2", MSG_COUNT)

    # Restart and immediately flood the subject.
    # With the bug, messages arriving while ClickHouse loads tables may only
    # reach the MV that happened to load first.
    instance.restart_clickhouse()

    asyncio.run(
        publish_messages(started_cluster.nats_port, MSG_COUNT, start_val=MSG_COUNT)
    )

    total = MSG_COUNT * 2
    count1 = wait_for_count(instance, "nats_target1", total, timeout=60)
    count2 = wait_for_count(instance, "nats_target2", total, timeout=60)

    # Both targets must have received exactly the same number of messages.
    # Any divergence means one MV was skipped during the startup window.
    assert count1 == count2, (
        f"Data loss after restart: target1={count1}, target2={count2}. "
        f"One MV was skipped during server startup (StorageNATS::checkDependencies "
        f"must use getReadyDependentViews, not getDependentViews)."
    )
    assert count1 == total, f"target1 expected {total} rows, got {count1}"
