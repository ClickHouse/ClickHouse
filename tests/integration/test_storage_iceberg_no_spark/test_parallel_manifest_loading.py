"""
Integration tests for parallel Iceberg manifest file loading
(iceberg_metadata_files_parallel_loading_threads setting).

Test matrix
-----------
test_parallel_equivalence_and_branch_selection
                              — on a cold cache, results (count, sum, full rows) at threads>1
                                match the serial (threads=1) baseline, AND
                                IcebergManifestFilesParallelFetched is 0 for serial /
                                N_MANIFESTS for parallel (proves the right code path ran)
test_parallel_threads_setting — MaxThreads semantics (no clamp, no upper bound),
                                parametrised over {0, 1, 8}:
                                  threads=0  → auto (cores > 1) → parallel path (Fetched=N_MANIFESTS)
                                  threads=1  → serial path (Fetched=0)
                                  threads>1  → parallel path (Fetched=N_MANIFESTS)
"""

import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str


NUM_MANIFESTS = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_manifests(instance, table_name, n):
    """Insert n rows one at a time so each INSERT produces a separate manifest file."""
    for i in range(n):
        instance.query(
            f"INSERT INTO {table_name} VALUES ({i}, 'row_{i}')",
            settings={"allow_insert_into_iceberg": 1},
        )


def _get_profile_event(instance, query, settings, event_name, extra_tag):
    """
    Run *query* with logging enabled, flush logs, return the value of
    ProfileEvents[event_name] from system.query_log for that specific query.

    *extra_tag* uniquely identifies the measured query via the `log_comment`
    setting (a dedicated query_log column), so we never have to match on the
    query text.  Matching on text with `query LIKE '%tag%'` is unreliable: the
    reader SELECT below contains the tag too, so it self-matches its own row
    (all ProfileEvents = 0).  We tag via log_comment and additionally exclude
    any row that touches query_log so the reader can never pick itself.
    """
    instance.query(
        query,
        settings={**settings, "log_queries": 1, "log_comment": extra_tag},
    )
    instance.query("SYSTEM FLUSH LOGS")
    result = instance.query(
        f"SELECT ProfileEvents['{event_name}'] "
        f"FROM system.query_log "
        f"WHERE log_comment = '{extra_tag}' "
        f"  AND type = 'QueryFinish' "
        f"  AND query NOT LIKE '%query_log%' "
        f"ORDER BY event_time_microseconds DESC "
        f"LIMIT 1"
    )
    return int(result.strip() or "0")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def iceberg_table(started_cluster_iceberg_no_spark):
    """Create a 30-manifest IcebergLocal table for one test and drop it after."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "t_pml_" + get_uuid_str().replace("-", "")

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id UInt32, val String)",
        2,
    )
    _insert_manifests(instance, table_name, NUM_MANIFESTS)
    yield instance, table_name
    instance.query(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------------
# 1. Equivalence + branch selection
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("threads", [1, 16])
def test_parallel_equivalence_and_branch_selection(iceberg_table, threads):
    """
    On a cold metadata cache, for each thread count assert BOTH:
      (a) results (count, sum, full row content) are identical to the serial
          (threads=1) baseline, and
      (b) the right code path ran: IcebergManifestFilesParallelFetched == 0 for the
          serial path (threads=1) and == NUM_MANIFESTS for the parallel path (threads>1).
    A silently-serial implementation would pass (a) but fail (b).
    """
    instance, table_name = iceberg_table

    serial_settings = {
        "iceberg_metadata_files_parallel_loading_threads": 1,
        "use_iceberg_metadata_files_cache": 0,
    }
    run_settings = {
        "iceberg_metadata_files_parallel_loading_threads": threads,
        "use_iceberg_metadata_files_cache": 0,
    }

    # (a) results identical to the serial baseline (count, sum, full rows).
    serial_count = int(instance.query(f"SELECT count() FROM {table_name}", settings=serial_settings))
    run_count = int(instance.query(f"SELECT count() FROM {table_name}", settings=run_settings))
    assert serial_count == NUM_MANIFESTS
    assert run_count == serial_count

    expected_sum = NUM_MANIFESTS * (NUM_MANIFESTS - 1) // 2
    serial_sum = int(instance.query(f"SELECT sum(id) FROM {table_name}", settings=serial_settings))
    run_sum = int(instance.query(f"SELECT sum(id) FROM {table_name}", settings=run_settings))
    assert serial_sum == expected_sum
    assert run_sum == serial_sum

    serial_rows = instance.query(f"SELECT id, val FROM {table_name} ORDER BY id", settings=serial_settings)
    run_rows = instance.query(f"SELECT id, val FROM {table_name} ORDER BY id", settings=run_settings)
    assert run_rows == serial_rows

    # (b) branch selection — measure Fetched on a cold count() that actually exercises the
    # iterator. optimize_trivial_count_query must be off: a bare count() otherwise reads row
    # totals from manifest metadata and never instantiates the parallel data iterator.
    fetched = _get_profile_event(
        instance,
        f"SELECT count() FROM {table_name}",
        {
            "use_iceberg_metadata_files_cache": 0,
            "optimize_trivial_count_query": 0,
            "iceberg_metadata_files_parallel_loading_threads": threads,
        },
        "IcebergManifestFilesParallelFetched",
        get_uuid_str(),
    )
    if threads == 1:
        assert fetched == 0, (
            f"Serial path (threads=1) incremented IcebergManifestFilesParallelFetched "
            f"({fetched}); expected 0."
        )
    else:
        assert fetched == NUM_MANIFESTS, (
            f"Parallel path (threads={threads}) fetched {fetched} manifests via futures; "
            f"expected {NUM_MANIFESTS}."
        )


# ---------------------------------------------------------------------------
# 2. Threads setting (MaxThreads semantics: 0=auto, 1=serial, >1=parallel)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("threads", [0, 1, 8])
def test_parallel_threads_setting(iceberg_table, threads):
    """
    iceberg_metadata_files_parallel_loading_threads is a MaxThreads setting: there is no
    upper bound and no clamping. The value selects the code path:
      - threads=0 → auto (number of CPU cores), which is > 1 → parallel path, Fetched == N.
      - threads=1 → serial path, Fetched == 0.
      - threads>1 → parallel path, Fetched == N.
    """
    instance, table_name = iceberg_table
    base_query = f"SELECT count() FROM {table_name}"
    # optimize_trivial_count_query must be off: a bare count() otherwise reads row
    # totals from manifest metadata and never instantiates the parallel data iterator.
    no_cache = {"use_iceberg_metadata_files_cache": 0, "optimize_trivial_count_query": 0}

    fetched = _get_profile_event(
        instance,
        base_query,
        {**no_cache, "iceberg_metadata_files_parallel_loading_threads": threads},
        "IcebergManifestFilesParallelFetched",
        get_uuid_str(),
    )

    if threads == 1:
        assert fetched == 0, (
            f"threads=1 must use the serial path (IcebergManifestFilesParallelFetched=0), "
            f"got {fetched}."
        )
    else:
        # threads=0 maps to auto (cores > 1) and threads>1 both take the parallel path.
        assert fetched == NUM_MANIFESTS, (
            f"threads={threads} must use the parallel path "
            f"(IcebergManifestFilesParallelFetched={NUM_MANIFESTS}), got {fetched}."
        )
