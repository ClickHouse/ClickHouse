#!/usr/bin/env python3
"""Tests for Azure 403 error handling and RBAC warmup retry amplification."""
import os

import pytest

from helpers.cluster import ClickHouseCluster


AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONTAINER = "cont"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# --- Retry math for the test disks (azure_disk_default, azure_disk_warmup) ---
#
# Both disks: max_single_read_retries=3, max_single_download_retries=3,
#             max_unexpected_write_error_retries=3
#
# azure_disk_default: rbac_warmup_interval_sec=0 (disabled)
#   effective_tries = base = 3
#
# azure_disk_warmup: rbac_warmup_interval_sec=300, rbac_warmup_retry_multiplier=3
#   effective_tries = base * multiplier = 9
#
# Backoff: 100ms initial, doubles each attempt.
# Total backoff for N tries = 100 * (2^(N-1) - 1) ms
#   3 tries →   0.3s
#   9 tries →  25.5s
#  12 tries → 204.7s  (production write base=4, multiplier=3)
#
# Retry log messages per failed operation = effective_tries - 1
# (the last attempt throws without logging)
#
# Amplification table (production defaults):
#   Base  Mult  Effective  Backoff
#      3     1          3    0.3s    (no warmup)
#      3     3          9   25.5s
#      4     1          4    0.7s    (no warmup)
#      4     3         12    3.4m
#
# Max tries fitting within a warmup window:
#   30s  → max  9 tries  (base=3 → mult≤3, base=4 → mult≤2)
#    2m  → max 11 tries  (base=3 → mult≤3, base=4 → mult≤2)
#    5m  → max 12 tries  (base=3 → mult≤4, base=4 → mult≤3)
#   10m  → max 13 tries  (base=3 → mult≤4, base=4 → mult≤3)

BASE_RETRIES = 3
WARMUP_MULTIPLIER = 3
DEFAULT_EFFECTIVE = BASE_RETRIES
WARMUP_EFFECTIVE = BASE_RETRIES * WARMUP_MULTIPLIER


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[os.path.join(SCRIPT_DIR, "configs", "azure_disk.xml")],
    with_azurite=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True, scope="function")
def reset_failpoint_between_tests(started_cluster):
    try:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
    except Exception:
        pass
    yield
    try:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
    except Exception:
        pass


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


def _count_retry_messages_since(from_line):
    """Count 'failed at attempt' log messages in lines added after from_line."""
    cmd = (
        f"tail -n +{from_line + 1} /var/log/clickhouse-server/clickhouse-server.log "
        f'| grep -c "failed at attempt" || true'
    )
    result = node.exec_in_container(["bash", "-c", cmd])
    return int(result.strip())


def _count_warmup_annotations_since(from_line):
    """Count '(warmup, base N)' annotations in lines added after from_line."""
    cmd = (
        f"tail -n +{from_line + 1} /var/log/clickhouse-server/clickhouse-server.log "
        f'| grep -c "warmup, base" || true'
    )
    result = node.exec_in_container(["bash", "-c", cmd])
    return int(result.strip())


# ===== Existing tests (unchanged) =====


def test_sanity_check(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    _create_table(endpoint, "t", "basic.csv")
    node.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    assert node.query("SELECT count() FROM t").strip() == "3"
    assert node.query("SELECT v FROM t WHERE k = 2").strip() == "b"

    files = node.query(
        f"""
        SELECT DISTINCT _file
        FROM azureBlobStorage(
            '{endpoint}', '{CONTAINER}', '*',
            '{AZURITE_ACCOUNT}', '{AZURITE_KEY}', 'CSV'
        )
        ORDER BY _file
        FORMAT TabSeparated
        """
    ).strip().splitlines()
    assert files == ["basic.csv"]


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


# ===== Default (no warmup) happy-path tests =====


def test_default_happy_read_write(started_cluster):
    node.query(
        """
        CREATE TABLE t_default_happy_rw (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_default_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_default_happy_rw VALUES (1,'a'),(2,'b'),(3,'c')")

    _drop_caches()

    assert node.query("SELECT count() FROM t_default_happy_rw").strip() == "3"
    assert node.query("SELECT v FROM t_default_happy_rw WHERE k=2").strip() == "b"

    disk = node.query(
        "SELECT disk_name FROM system.parts "
        "WHERE table='t_default_happy_rw' AND active LIMIT 1"
    ).strip()
    assert disk == "azure_disk_default", f"expected azure_disk_default, got {disk}"


def test_default_happy_mutate(started_cluster):
    node.query(
        """
        CREATE TABLE t_default_happy_mut (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_default_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_default_happy_mut VALUES (1,'a'),(2,'b'),(3,'c')")

    node.query(
        "ALTER TABLE t_default_happy_mut UPDATE v='x' WHERE k=2 "
        "SETTINGS mutations_sync=1"
    )

    _drop_caches()

    assert node.query(
        "SELECT v FROM t_default_happy_mut WHERE k=2"
    ).strip() == "x"


# ===== Default (no warmup) negative tests =====


def test_default_negative_read_write(started_cluster):
    node.query(
        """
        CREATE TABLE t_default_neg_rw (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_default_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_default_neg_rw VALUES (1,'a'),(2,'b')")

    _drop_caches()

    log_before = node.count_log_lines()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        # SELECT count() uses MergeTree metadata without Azure reads.
        # Force actual data read by selecting column values.
        err = node.query_and_get_error("SELECT v FROM t_default_neg_rw FORMAT Null")
        assert "403" in err or "Forbidden" in err, f"expected 403 error, got:\n{err}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    retry_count = _count_retry_messages_since(log_before)
    expected = DEFAULT_EFFECTIVE - 1
    assert retry_count >= expected, (
        f"expected at least {expected} retry messages "
        f"(base={BASE_RETRIES}, no warmup), got {retry_count}"
    )

    warmup_count = _count_warmup_annotations_since(log_before)
    assert warmup_count == 0, (
        f"expected no warmup annotations with default config, got {warmup_count}"
    )


def test_default_negative_mutate(started_cluster):
    node.query(
        """
        CREATE TABLE t_default_neg_mut (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_default_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(
            f"INSERT INTO t_default_neg_mut "
            f"SELECT number + {i * 100}, toString(number) FROM numbers(100)"
        )
    assert node.query("SELECT count() FROM t_default_neg_mut").strip() == "300"

    _drop_caches()

    log_before = node.count_log_lines()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("OPTIMIZE TABLE t_default_neg_mut FINAL")
        assert "403" in err or "Forbidden" in err, f"expected 403 error, got:\n{err}"
        assert "POTENTIALLY_BROKEN_DATA_PART" not in err
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    retry_count = _count_retry_messages_since(log_before)
    expected = DEFAULT_EFFECTIVE - 1
    assert retry_count >= expected, (
        f"expected at least {expected} retry messages "
        f"(base={BASE_RETRIES}, no warmup), got {retry_count}"
    )

    warmup_count = _count_warmup_annotations_since(log_before)
    assert warmup_count == 0, (
        f"expected no warmup annotations with default config, got {warmup_count}"
    )


# ===== Configured (warmup active) happy-path tests =====


def test_configured_happy_read_write(started_cluster):
    node.query(
        """
        CREATE TABLE t_warmup_happy_rw (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_warmup_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_warmup_happy_rw VALUES (1,'a'),(2,'b'),(3,'c')")

    _drop_caches()

    assert node.query("SELECT count() FROM t_warmup_happy_rw").strip() == "3"
    assert node.query("SELECT v FROM t_warmup_happy_rw WHERE k=2").strip() == "b"

    disk = node.query(
        "SELECT disk_name FROM system.parts "
        "WHERE table='t_warmup_happy_rw' AND active LIMIT 1"
    ).strip()
    assert disk == "azure_disk_warmup", f"expected azure_disk_warmup, got {disk}"


def test_configured_happy_mutate(started_cluster):
    node.query(
        """
        CREATE TABLE t_warmup_happy_mut (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_warmup_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_warmup_happy_mut VALUES (1,'a'),(2,'b'),(3,'c')")

    node.query(
        "ALTER TABLE t_warmup_happy_mut UPDATE v='x' WHERE k=2 "
        "SETTINGS mutations_sync=1"
    )

    _drop_caches()

    assert node.query(
        "SELECT v FROM t_warmup_happy_mut WHERE k=2"
    ).strip() == "x"


# ===== Configured (warmup active) negative tests =====


def test_configured_negative_read_write(started_cluster):
    node.query(
        """
        CREATE TABLE t_warmup_neg_rw (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_warmup_policy', min_bytes_for_wide_part = 0
        """
    )
    node.query("INSERT INTO t_warmup_neg_rw VALUES (1,'a'),(2,'b')")

    _drop_caches()

    log_before = node.count_log_lines()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("SELECT v FROM t_warmup_neg_rw FORMAT Null")
        assert "403" in err or "Forbidden" in err, f"expected 403 error, got:\n{err}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    retry_count = _count_retry_messages_since(log_before)
    expected = WARMUP_EFFECTIVE - 1
    assert retry_count >= expected, (
        f"expected at least {expected} retry messages "
        f"(base={BASE_RETRIES}, multiplier={WARMUP_MULTIPLIER}), got {retry_count}"
    )

    warmup_count = _count_warmup_annotations_since(log_before)
    assert warmup_count >= expected, (
        f"expected at least {expected} warmup annotations, got {warmup_count}"
    )


def test_configured_negative_mutate(started_cluster):
    node.query(
        """
        CREATE TABLE t_warmup_neg_mut (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_warmup_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(
            f"INSERT INTO t_warmup_neg_mut "
            f"SELECT number + {i * 100}, toString(number) FROM numbers(100)"
        )
    assert node.query("SELECT count() FROM t_warmup_neg_mut").strip() == "300"

    _drop_caches()

    log_before = node.count_log_lines()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("OPTIMIZE TABLE t_warmup_neg_mut FINAL")
        assert "403" in err or "Forbidden" in err, f"expected 403 error, got:\n{err}"
        assert "POTENTIALLY_BROKEN_DATA_PART" not in err
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")

    retry_count = _count_retry_messages_since(log_before)
    expected = WARMUP_EFFECTIVE - 1
    assert retry_count >= expected, (
        f"expected at least {expected} retry messages "
        f"(base={BASE_RETRIES}, multiplier={WARMUP_MULTIPLIER}), got {retry_count}"
    )

    warmup_count = _count_warmup_annotations_since(log_before)
    assert warmup_count >= expected, (
        f"expected at least {expected} warmup annotations, got {warmup_count}"
    )
