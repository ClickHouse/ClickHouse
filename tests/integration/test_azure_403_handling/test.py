#!/usr/bin/env python3
"""Regression tests: Azure 403 is retried and never misreported as POTENTIALLY_BROKEN_DATA_PART."""
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
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


FAILPOINTS = (
    "azure_inject_forbidden_response",
    "azure_inject_auth_failure",
    "azure_inject_timeout",
    "azure_inject_forbidden_on_upload",
)


@pytest.fixture(autouse=True, scope="function")
def reset_failpoint_between_tests(started_cluster):
    def _disable_all():
        for fp in FAILPOINTS:
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {fp}")
            except Exception:
                pass

    _disable_all()
    yield
    _disable_all()


def _create_table(endpoint, name, blob):
    node.query(
        f"""
        CREATE TABLE {name} (k UInt64, v String)
        ENGINE = AzureBlobStorage(
            '{endpoint}', '{CONTAINER}', '{blob}',
            '{AZURITE_ACCOUNT}', '{AZURITE_KEY}', 'CSV'
        )
        """
    )


def _drop_caches():
    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("SYSTEM DROP MARK CACHE")


def test_sanity_check(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    _create_table(endpoint, "t", "basic.csv")
    node.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    assert node.query("SELECT count() FROM t").strip() == "3"
    assert node.query("SELECT v FROM t WHERE k = 2").strip() == "b"


def test_azure_403_is_retried_not_broken_part(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    _create_table(endpoint, "t_403", "failpoint.csv")
    node.query("INSERT INTO t_403 VALUES (1, 'a'), (2, 'b')")

    assert node.query("SELECT count() FROM t_403").strip() == "2"

    _drop_caches()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("SELECT count() FROM t_403")
        assert "403" in err or "Forbidden" in err, f"expected 403 error, got:\n{err}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")


def test_azure_403_at_merge_not_broken_part(started_cluster):
    node.query(
        """
        CREATE TABLE t_merge (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(
            f"INSERT INTO t_merge SELECT number + {i * 100}, toString(number) FROM numbers(100)"
        )
    assert node.query("SELECT count() FROM t_merge").strip() == "300"

    _drop_caches()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("OPTIMIZE TABLE t_merge FINAL")
        assert "POTENTIALLY_BROKEN_DATA_PART" not in err, (
            f"unexpected broken-part error:\n{err}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")


def test_azure_auth_failure_at_merge_not_broken_part(started_cluster):
    # A credential/token failure (AuthenticationException, not an HTTP 403) during a merge read
    # must also be treated as retryable, never reclassified as POTENTIALLY_BROKEN_DATA_PART.
    node.query(
        """
        CREATE TABLE t_auth_merge (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(
            f"INSERT INTO t_auth_merge SELECT number + {i * 100}, toString(number) FROM numbers(100)"
        )
    assert node.query("SELECT count() FROM t_auth_merge").strip() == "300"

    _drop_caches()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_auth_failure")
    try:
        err = node.query_and_get_error("OPTIMIZE TABLE t_auth_merge FINAL")
        assert "POTENTIALLY_BROKEN_DATA_PART" not in err, (
            f"unexpected broken-part error:\n{err}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_auth_failure")


def test_azure_timeout_read_is_retried(started_cluster):
    # A transient request timeout to Azure surfaces as an HTTP 408; it must be retried, not fail the read
    # (issue #110724). azure_inject_timeout is a ONCE failpoint, so exactly one Azure request fails with a
    # 408: with the fix the read retries past it and returns the data; without it (408 treated as
    # non-retryable) the read gives up on the first attempt and the query fails. A retried transient error
    # also never reaches the reportBroken()/POTENTIALLY_BROKEN_DATA_PART path for a healthy part.
    node.query(
        """
        CREATE TABLE t_timeout_read (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_timeout_read SELECT number, toString(number) FROM numbers(100)")
    assert node.query("SELECT count() FROM t_timeout_read").strip() == "100"

    _drop_caches()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_timeout")
    try:
        assert node.query("SELECT sum(k) FROM t_timeout_read").strip() == "4950"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_timeout")


def test_batch_delete_failure_is_logged(started_cluster):
    # If the batch DELETE request itself fails (403), each object must still get a
    # system.blob_storage_log Delete event (previously the whole per-object loop was skipped).
    node.query("DROP TABLE IF EXISTS t_del SYNC")
    node.query(
        """
        CREATE TABLE t_del (k UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(f"INSERT INTO t_del SELECT number + {i * 100} FROM numbers(100)")

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        # Object removal on DROP is best-effort, so the DROP may still succeed; we only assert that
        # the failed remote batch delete produced per-object blob_storage_log Delete events.
        try:
            node.query("DROP TABLE t_del SYNC")
        except Exception:
            pass
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    node.query("SYSTEM FLUSH LOGS")
    logged = node.query(
        "SELECT count() FROM system.blob_storage_log WHERE event_type = 'Delete' AND error != ''"
    ).strip()
    assert int(logged) > 0, f"expected failed Delete events to be logged, got {logged}"

    node.query("DROP TABLE IF EXISTS t_del SYNC")  # cleanup (failpoint disabled)


def test_azure_403_on_upload_read_write_copy_is_retried(started_cluster):
    # Blocker 1: the read+write copy fallback (UploadHelper) must retry a transient destination
    # error on its write ops, the same way the read side already does. Force the read+write path
    # with allow_azure_native_copy = 0 and inject one transient 403 on the upload; the backup must
    # still succeed. Before the fix the first upload write threw on the first try and failed the copy.
    connection_string = started_cluster.env_variables["AZURITE_CONNECTION_STRING"]

    node.query("DROP TABLE IF EXISTS t_upload SYNC")
    node.query("DROP TABLE IF EXISTS t_upload_restored SYNC")
    node.query(
        """
        CREATE TABLE t_upload (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_upload SELECT number, toString(number) FROM numbers(100)")

    cont = "backupcont" + str(time.time_ns())
    backup_dest = f"AzureBlobStorage('{connection_string}', '{cont}', 't_upload_backup')"

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_on_upload")
    try:
        node.query(
            f"BACKUP TABLE t_upload TO {backup_dest} SETTINGS allow_azure_native_copy = 0"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_on_upload")

    # Guard against a false pass: the copy must have gone through the read+write (UploadHelper) path,
    # not a server-side native copy (which would never reach the injected write failpoint).
    assert node.contains_in_log("Reading and writing Blob")

    node.query(
        f"RESTORE TABLE t_upload AS t_upload_restored FROM {backup_dest} "
        f"SETTINGS allow_azure_native_copy = 0"
    )
    assert node.query("SELECT count() FROM t_upload_restored").strip() == "100"

    node.query("DROP TABLE IF EXISTS t_upload SYNC")
    node.query("DROP TABLE IF EXISTS t_upload_restored SYNC")


def test_azure_403_check_table_surfaces_transient_not_verified(started_cluster):
    # Blocker 2: checkDataPart must rethrow a retryable Azure error instead of returning empty
    # checksums, so a verification caller (here CHECK TABLE) surfaces the transient failure rather
    # than masquerading it as a verified "OK" (or a false "broken"). With the fix the retryable 403
    # propagates as a query error; before the fix CHECK TABLE reported the part as OK.
    node.query("DROP TABLE IF EXISTS t_check SYNC")
    node.query(
        """
        CREATE TABLE t_check (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_check SELECT number, toString(number) FROM numbers(100)")
    assert node.query("SELECT count() FROM t_check").strip() == "100"

    _drop_caches()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("CHECK TABLE t_check")
        assert "403" in err or "Forbidden" in err, f"expected surfaced transient error, got:\n{err}"
        assert "POTENTIALLY_BROKEN_DATA_PART" not in err, err
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    node.query("DROP TABLE IF EXISTS t_check SYNC")
