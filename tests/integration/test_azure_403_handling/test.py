#!/usr/bin/env python3
"""Azure transient-vs-permanent error handling on the MergeTree read/merge path.

Covers #106298, #110724 and private #53656: a transient Azure 403, credential failure or connect
timeout must be retried and must never be attributed to a broken data part; a permanent one must
fail with the real Azure error and still not accuse the part. A genuinely non-retryable error must
still mark the part broken.

ReplicatedMergeTree so reportBroken() is observable (plain MergeTree's callback is a no-op).
"""
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONTAINER = "cont"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        os.path.join(SCRIPT_DIR, "configs", "azure_disk.xml"),
        os.path.join(SCRIPT_DIR, "configs", "blob_log.xml"),
    ],
    with_azurite=True,
    with_zookeeper=True,
)

# kind -> (one-shot fp, permanent fp, expected error text)
ERROR_KINDS = {
    "forbidden": (
        "azure_inject_forbidden_response_once",
        "azure_inject_forbidden_response",
        "403",
    ),
    "auth": (
        "azure_inject_auth_failure_on_request_once",
        "azure_inject_auth_failure_on_request",
        "AuthenticationException",
    ),
    "timeout": (
        "azure_inject_poco_timeout_once",
        "azure_inject_poco_timeout",
        "TransportException",
    ),
    # Poco network/IO error (connection reset) -> TransportException, like timeout (r3767770811).
    "network": (
        "azure_inject_poco_network_error_once",
        "azure_inject_poco_network_error",
        "TransportException",
    ),
}

ALL_FAILPOINTS = [fp for triple in ERROR_KINDS.values() for fp in triple[:2]] + [
    "azure_inject_bad_request",
    "azure_inject_forbidden_response_on_put_once",
]

# The OSS-observable signal that reportBroken() was taken (part-check thread).
BROKEN_PART_LOG = "looks broken. Removing it and will try to fetch"
# One line per failed Azure read attempt; substring matches both Read and Download variants.
RETRY_LOG = "Exception caught during Azure"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True, scope="function")
def clean_state(started_cluster):
    def _disable_all():
        for fp in ALL_FAILPOINTS:
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {fp}")
            except Exception:
                pass

    _disable_all()
    node.rotate_logs()
    yield
    _disable_all()


def _create_table(name, wide=True, stop_merges=False, policy="azure_policy"):
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    part_setting = (
        "min_bytes_for_wide_part = 0" if wide else "min_bytes_for_wide_part = 1073741824"
    )
    node.query(
        f"""
        CREATE TABLE {name} (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{name}', 'r1')
        ORDER BY k
        SETTINGS storage_policy = '{policy}', {part_setting}
        """
    )
    # Three distinct parts so OPTIMIZE has real merge work that reaches the read (the reportBroken() decision).
    if stop_merges:
        node.query(f"SYSTEM STOP MERGES {name}")
    for i in range(3):
        node.query(
            f"INSERT INTO {name} SELECT number + {i * 100}, toString(number) FROM numbers(100)"
        )
    assert node.query(f"SELECT count() FROM {name}").strip() == "300"
    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("SYSTEM DROP MARK CACHE")


def _wait_for_merge_failure(table, expected_in_err, timeout=90):
    # A retryable merge failure retries indefinitely — the only terminal signal is last_exception on the
    # replication queue; wait for it (failpoint still on) so the no-broken-part check is meaningful.
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        last = node.query(
            f"SELECT last_exception FROM system.replication_queue "
            f"WHERE database = currentDatabase() AND table = '{table}'"
        )
        if expected_in_err in last:
            return
        time.sleep(0.5)
    raise AssertionError(
        f"merge for {table} never recorded a failure containing {expected_in_err!r}; "
        f"last_exception=\n{last}"
    )


def test_sanity_check(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    node.query(
        f"""
        CREATE TABLE t_sanity (k UInt64, v String)
        ENGINE = AzureBlobStorage('{endpoint}', '{CONTAINER}', 'sanity.csv',
                                  '{AZURITE_ACCOUNT}', '{AZURITE_KEY}', 'CSV')
        """
    )
    node.query("INSERT INTO t_sanity VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    assert node.query("SELECT count() FROM t_sanity").strip() == "3"


def test_sdk_retry_isolates_forbidden_on_direct_call(started_cluster):
    # Isolation test for the SDK 403 retry (StatusCodes.insert(Forbidden)): an INSERT's direct GetProperties/
    # exists() calls have no CH retry loop, so this one-shot 403 succeeds only if the SDK retried it — drop the line, it fails.
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response_once")
    try:
        node.query(
            f"""
            INSERT INTO TABLE FUNCTION azureBlobStorage(
                '{endpoint}', '{CONTAINER}', 'b1_direct_probe.csv',
                '{AZURITE_ACCOUNT}', '{AZURITE_KEY}', 'CSV', 'auto', 'k UInt64')
            VALUES (1)
            """
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response_once")


@pytest.mark.parametrize("kind", list(ERROR_KINDS))
def test_transient_error_read_succeeds(started_cluster, kind):
    # One transient failure must be absorbed by the retry budget: data still returned, part never broken.
    # No RETRY_LOG assert — the SDK may absorb the one-shot below the ClickHouse loop.
    once_fp, _, _ = ERROR_KINDS[kind]
    table = f"t_transient_{kind}"
    _create_table(table)

    node.query(f"SYSTEM ENABLE FAILPOINT {once_fp}")
    try:
        assert node.query(f"SELECT sum(k) FROM {table}").strip() == "44850"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {once_fp}")

    assert not node.contains_in_log(BROKEN_PART_LOG)


@pytest.mark.parametrize("kind", list(ERROR_KINDS))
def test_permanent_error_read_fails_without_accusing_part(started_cluster, kind):
    # A permanent error fails with the real Azure error after the budget, and still never accuses the part.
    _, perm_fp, expected_in_err = ERROR_KINDS[kind]
    table = f"t_permanent_{kind}"
    _create_table(table)

    node.query(f"SYSTEM ENABLE FAILPOINT {perm_fp}")
    try:
        err = node.query_and_get_error(f"SELECT sum(k) FROM {table}")
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {perm_fp}")

    assert expected_in_err in err, f"expected {expected_in_err} in error, got:\n{err}"
    # Load-bearing: the healthy part must never be reported broken. 740 is private-only, so we assert on
    # BROKEN_PART_LOG (ReplicatedMergeTreePartCheckThread) — the OSS-observable reportBroken() signal.
    assert not node.contains_in_log(BROKEN_PART_LOG)
    assert node.contains_in_log(RETRY_LOG), "the read retry budget was never used"


@pytest.mark.parametrize("kind", list(ERROR_KINDS))
def test_permanent_error_at_merge_does_not_mark_part_broken(started_cluster, kind):
    # Permanent error at merge -> merge reschedules (async, alter_sync=0), part never marked broken.
    _, perm_fp, expected_in_err = ERROR_KINDS[kind]
    table = f"t_merge_{kind}"
    _create_table(table, stop_merges=True)

    node.query(f"SYSTEM ENABLE FAILPOINT {perm_fp}")
    try:
        node.query(f"SYSTEM START MERGES {table}")
        node.query(f"OPTIMIZE TABLE {table} FINAL SETTINGS alter_sync = 0")
        # Keep the failpoint on until the merge's terminal failure: a false reportBroken() on the final attempt
        # would slip past an early RETRY_LOG check (and past count()==300, since refetch restores the rows).
        _wait_for_merge_failure(table, expected_in_err, timeout=90)
        assert not node.contains_in_log(BROKEN_PART_LOG)
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {perm_fp}")


def test_transient_forbidden_compact_part_read_succeeds(started_cluster):
    # Cover a compact part too — #106298's stack is the compact reader, a different reportBroken() site.
    once_fp, _, _ = ERROR_KINDS["forbidden"]
    _create_table("t_compact", wide=False)

    node.query(f"SYSTEM ENABLE FAILPOINT {once_fp}")
    try:
        assert node.query("SELECT sum(k) FROM t_compact").strip() == "44850"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {once_fp}")

    assert not node.contains_in_log(BROKEN_PART_LOG)


def test_non_retryable_error_still_marks_part_broken(started_cluster):
    # Negative control: a non-retryable 400 must still reportBroken(), else real corruption is missed.
    _create_table("t_negative")

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_bad_request")
    try:
        node.query_and_get_error("SELECT sum(k) FROM t_negative")
        node.wait_for_log_line(BROKEN_PART_LOG, timeout=60)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_bad_request")


def test_permanent_forbidden_on_write_fails(started_cluster):
    # 403 is retryable on writes now too; a permanent one must still fail on the bounded budget, not hang.
    _create_table("t_write")

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error(
            "INSERT INTO t_write SELECT number, toString(number) FROM numbers(100)"
        )
        assert "403" in err or "Forbidden" in err
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")


def test_transient_forbidden_on_write_succeeds(started_cluster):
    # nosdk (SDK retry off) so the one-shot 403 must reach execWithRetry; the "Write at attempt" log proves the
    # CH write loop recovered. The one-shot fires only on a blob PUT, so the read below cannot take it.
    _create_table("t_write_transient", stop_merges=True, policy="azure_policy_nosdk")

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response_on_put_once")
    try:
        assert node.query("SELECT sum(k) FROM t_write_transient").strip() == "44850"
        node.query(
            "INSERT INTO t_write_transient SELECT number + 300, toString(number) FROM numbers(100)"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response_on_put_once")

    assert node.query("SELECT count() FROM t_write_transient").strip() == "400"
    assert node.contains_in_log(
        "Write at attempt"
    ), "the CH-level write retry loop was never exercised"
    assert not node.contains_in_log(BROKEN_PART_LOG)


def test_batch_delete_failure_logs_every_object(started_cluster):
    # When the batch DELETE request itself fails (here a permanent 403), the per-object response loop is
    # skipped, so removeObjectsBatchIfExists must still emit one system.blob_storage_log Delete event per
    # object before rethrowing — otherwise those deletes vanish from the audit log. Assert exactly that:
    # one failed Delete row per blob, scoped to this table's own objects, carrying the real Azure status
    # code (403), not the placeholder -1.
    node.query("DROP TABLE IF EXISTS t_batch_del SYNC")
    node.query(
        """
        CREATE TABLE t_batch_del (k UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(f"INSERT INTO t_batch_del SELECT number + {i * 100} FROM numbers(100)")

    # The exact remote blobs backing this table, scoped by its UUID so residue from other tests in the
    # module-scoped, append-only blob_storage_log cannot satisfy the assertion on its own.
    table_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_batch_del'"
    ).strip()
    expected_blobs = set(
        node.query(
            "SELECT remote_path FROM system.remote_data_paths "
            f"WHERE disk_name = 'azure_disk' AND local_path LIKE '%{table_uuid}%'"
        ).split()
    )
    assert expected_blobs, "the table must be backed by remote objects"

    logged = set()
    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        # Object removal on DROP is best-effort (and can run asynchronously), so the DROP itself may or
        # may not surface the error. Keep the failpoint enabled and poll until every object's failed
        # Delete event has been recorded, matching only this table's blobs and the injected 403.
        try:
            node.query("DROP TABLE t_batch_del SYNC")
        except Exception:
            pass

        blob_list = ", ".join(f"'{p}'" for p in expected_blobs)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            node.query("SYSTEM FLUSH LOGS")
            logged = set(
                node.query(
                    "SELECT remote_path FROM system.blob_storage_log "
                    "WHERE event_type = 'Delete' AND error_code = 403 "
                    f"AND remote_path IN ({blob_list})"
                ).split()
            )
            if logged >= expected_blobs:
                break
            time.sleep(0.5)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    missing = expected_blobs - logged
    assert not missing, (
        f"expected one failed Delete event (error_code=403) per object, "
        f"missing {len(missing)}/{len(expected_blobs)}: {sorted(missing)[:5]}"
    )

    node.query("DROP TABLE IF EXISTS t_batch_del SYNC")  # cleanup (failpoint disabled)


def test_batch_delete_non_azure_failure_logs_every_object(started_cluster):
    # Negative control for the catch(...) fallback in removeObjectsBatchIfExists: when SubmitBatch fails
    # with a NON-Azure exception (an injected credential AuthenticationException, a std::exception that is
    # NOT an Azure::Storage::StorageException), the batch path must still emit one Delete event per object
    # before rethrowing, carrying the placeholder error_code = -1 (no HTTP status) plus the exception text.
    node.query("DROP TABLE IF EXISTS t_batch_del_auth SYNC")
    node.query(
        """
        CREATE TABLE t_batch_del_auth (k UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(f"INSERT INTO t_batch_del_auth SELECT number + {i * 100} FROM numbers(100)")

    table_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_batch_del_auth'"
    ).strip()
    expected_blobs = set(
        node.query(
            "SELECT remote_path FROM system.remote_data_paths "
            f"WHERE disk_name = 'azure_disk' AND local_path LIKE '%{table_uuid}%'"
        ).split()
    )
    assert expected_blobs, "the table must be backed by remote objects"

    logged = set()
    node.query("SYSTEM ENABLE FAILPOINT azure_inject_auth_failure_on_request")
    try:
        try:
            node.query("DROP TABLE t_batch_del_auth SYNC")
        except Exception:
            pass

        blob_list = ", ".join(f"'{p}'" for p in expected_blobs)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            node.query("SYSTEM FLUSH LOGS")
            logged = set(
                node.query(
                    "SELECT remote_path FROM system.blob_storage_log "
                    "WHERE event_type = 'Delete' AND error_code = -1 "
                    f"AND remote_path IN ({blob_list})"
                ).split()
            )
            if logged >= expected_blobs:
                break
            time.sleep(0.5)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_auth_failure_on_request")

    missing = expected_blobs - logged
    assert not missing, (
        f"expected one failed Delete event (error_code=-1) per object, "
        f"missing {len(missing)}/{len(expected_blobs)}: {sorted(missing)[:5]}"
    )

    blob_list = ", ".join(f"'{p}'" for p in expected_blobs)
    errors = node.query(
        "SELECT DISTINCT error FROM system.blob_storage_log "
        "WHERE event_type = 'Delete' AND error_code = -1 "
        f"AND remote_path IN ({blob_list})"
    )
    assert "Authentication" in errors, f"expected the auth exception text in the log, got: {errors!r}"

    node.query("DROP TABLE IF EXISTS t_batch_del_auth SYNC")  # cleanup (failpoint disabled)


def test_batch_delete_failure_counts_profile_events(started_cluster):
    # Bot finding r3773675098: removeObjectsBatchIfExists must count the batch in system.events
    # (AzureDeleteObjects) even when the batch SubmitBatch itself fails. The increment now runs before the
    # submit — mirroring the single-object removeObjectImpl and the S3 delete path — so a failed batch is
    # still reflected in system.events, not only in system.blob_storage_log. Pre-fix (increment after the
    # try/catch) the rethrow skipped it, so this counter stayed flat while the blobs were still logged;
    # assert the counter now grows by at least one per object on the failed batch.
    node.query("DROP TABLE IF EXISTS t_batch_del_events SYNC")
    node.query(
        """
        CREATE TABLE t_batch_del_events (k UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(f"INSERT INTO t_batch_del_events SELECT number + {i * 100} FROM numbers(100)")

    table_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_batch_del_events'"
    ).strip()
    expected_blobs = set(
        node.query(
            "SELECT remote_path FROM system.remote_data_paths "
            f"WHERE disk_name = 'azure_disk' AND local_path LIKE '%{table_uuid}%'"
        ).split()
    )
    assert expected_blobs, "the table must be backed by remote objects"

    def azure_delete_objects():
        # sum() so an absent event row reads as 0; system.events is a live global counter (no flush needed).
        return int(
            node.query(
                "SELECT sum(value) FROM system.events WHERE event = 'AzureDeleteObjects'"
            ).strip()
        )

    events_before = azure_delete_objects()

    logged = set()
    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        try:
            node.query("DROP TABLE t_batch_del_events SYNC")
        except Exception:
            pass

        # Synchronize on the batch actually having been submitted: the increment now runs before the
        # catch's blob_storage_log entries, so once every object's failed Delete row is visible the
        # counter has definitely moved. Reuse the proven 403-batch poll for that barrier.
        blob_list = ", ".join(f"'{p}'" for p in expected_blobs)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            node.query("SYSTEM FLUSH LOGS")
            logged = set(
                node.query(
                    "SELECT remote_path FROM system.blob_storage_log "
                    "WHERE event_type = 'Delete' AND error_code = 403 "
                    f"AND remote_path IN ({blob_list})"
                ).split()
            )
            if logged >= expected_blobs:
                break
            time.sleep(0.5)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    missing = expected_blobs - logged
    assert not missing, (
        f"batch delete did not run for every object; missing {len(missing)}/{len(expected_blobs)}: "
        f"{sorted(missing)[:5]}"
    )

    delta = azure_delete_objects() - events_before
    assert delta >= len(expected_blobs), (
        f"expected system.events AzureDeleteObjects to grow by >= {len(expected_blobs)} "
        f"on the failed batch delete, got delta {delta}"
    )

    node.query("DROP TABLE IF EXISTS t_batch_del_events SYNC")  # cleanup (failpoint disabled)
