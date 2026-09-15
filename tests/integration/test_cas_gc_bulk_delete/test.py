import math
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_gc_bulk"
MANIFESTS_PREFIX = "cas_gc_bulk/cas/manifests/"
RECLAIM_RETRIES = 90
RECLAIM_SLEEP = 1.0

# Must match configs/storage_conf.xml's <cas_gc_bulk_delete_chunk_keys>: kept small on purpose so
# the dropped table's owner-removed manifests (>= NUM_INSERTS of them) span more than one chunk,
# which is what this test is proving on the wire.
CHUNK_KEYS = 2
NUM_INSERTS = 6


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node",
        main_configs=["configs/storage_conf.xml"],
        with_rustfs=True,
        stay_alive=True,
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def list_manifests():
    return [
        o.object_name
        for o in cluster.rustfs_client.list_objects(
            cluster.rustfs_bucket, MANIFESTS_PREFIX, recursive=True
        )
    ]


def gc_manifest_delete_totals(node):
    """
    Sum every `manifest_deletes` phase row with work across however many rounds it took: the
    owner-removed manifests from one DROP TABLE are not guaranteed to fold in a single round, so
    reading only the first row would silently under-count on a slow run.

    Chunking happens per round, not over the grand total, so the expected request count is the
    sum over rows of `ceil(row_attempted / CHUNK_KEYS)`, not `ceil(total_attempted / CHUNK_KEYS)`.
    """
    node.query("SYSTEM FLUSH LOGS")
    rows = (
        node.query(
            "SELECT phase_metrics['attempted'], phase_metrics['accepted'], phase_metrics['requests'], "
            "ProfileEvents['CASBulkDeleteRequests'], ProfileEvents['DiskS3DeleteObjects'] "
            "FROM system.cas_gc_log WHERE event_type = 'Phase' AND phase = 'manifest_deletes' "
            "AND phase_metrics['attempted'] > 0"
        )
        .strip()
        .splitlines()
    )
    totals = [0, 0, 0, 0, 0]
    expected_requests = 0
    for row in rows:
        values = [int(x) for x in row.split("\t")]
        for i, value in enumerate(values):
            totals[i] += value
        expected_requests += math.ceil(values[0] / CHUNK_KEYS)
    # attempted, accepted, requests, bulk_requests, s3_deletes, expected_requests
    return tuple(totals) + (expected_requests,)


def test_manifest_deletes_go_in_one_request_per_chunk():
    """
    A dropped table's owner-removed manifest bodies are deleted through `manifest_deletes` in
    chunks of `cas_gc_bulk_delete_chunk_keys` write-once keys, one `DeleteObjects` request per
    chunk -- never one request per key. With the chunk size forced down to 2 and >= NUM_INSERTS
    manifests to delete, this asserts the request count matches the chunking math exactly, and
    that the engine's own `CASBulkDeleteRequests` / `DiskS3DeleteObjects` counters agree with it.
    """
    node = cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        f"CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k "
        f"SETTINGS storage_policy = '{STORAGE_POLICY}'"
    )
    for i in range(NUM_INSERTS):
        node.query(
            "INSERT INTO t SELECT number, toString(number) FROM numbers(1000) "
            "SETTINGS max_insert_block_size = 1000"
        )
    manifests_before = list_manifests()
    assert len(manifests_before) >= NUM_INSERTS

    node.query("DROP TABLE t SYNC")

    attempted = accepted = requests = bulk_requests = s3_deletes = expected_requests = 0
    for _ in range(RECLAIM_RETRIES):
        node.query("SYSTEM CAS GC RUN")
        (
            attempted,
            accepted,
            requests,
            bulk_requests,
            s3_deletes,
            expected_requests,
        ) = gc_manifest_delete_totals(node)
        if attempted >= NUM_INSERTS:
            break
        time.sleep(RECLAIM_SLEEP)
    assert attempted >= NUM_INSERTS, "manifest_deletes never reported the dropped table's manifests"

    assert accepted == attempted, "every owner-removed manifest body is deleted or already absent"
    assert requests == expected_requests, (
        f"chunking is per round, so {attempted} keys at {CHUNK_KEYS} per chunk across however "
        f"many rounds it took should sum to {expected_requests} requests, got {requests}"
    )
    assert bulk_requests == requests
    assert s3_deletes == requests, "one DeleteObjects per chunk, no singular fallback"

    for _ in range(RECLAIM_RETRIES):
        if not list_manifests():
            break
        node.query("SYSTEM CAS GC RUN")
        time.sleep(RECLAIM_SLEEP)
    assert not list_manifests()
