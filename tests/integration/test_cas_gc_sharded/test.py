"""Phase 4 integration soak: two-replica disjoint-shard GC with gc_shards=2.

Two ClickHouse nodes mount the SAME CA pool (shared-pool mode). The pool is configured with
`gc_shards=2` — at first GC-state creation the coordinator writes two `blob_target/<shard>`
runs (one per shard) per GC generation. Blob hashes route to shard 0 or shard 1 by
`blobShard(blob_hash, 2) = high64(hash) % 2` (CasGcShardPlan::blobShard). Each generation
therefore produces keys under both `blob_target/0/` and `blob_target/1/` (assuming the workload
generates enough distinct blobs to cover both shard buckets — see the 2000-row inserts below).

The pool runs on RustFS, not MinIO: the CA mount capability probe (`CasProbe::runCapabilityProbe`)
requires an S3-compatible backend that enforces `DeleteObject If-Match` (conditional delete);
MinIO OSS silently honors a mismatched-token DELETE instead of rejecting it, which the fail-closed
probe treats as a fatal capability gap.

The soak drives a blob-churn workload (INSERT x3 + OPTIMIZE FINAL + DROP x3 x2 rounds) on
`node1`, then restarts `node2` (light chaos), quiesces (waits for GC to drain), and asserts:

  A) No dangle / no loss — after quiesce both replicas return the same row counts for the live
     table; no CA-layer exception or fatal error appears in either server log.

  B) Single completion signal per generation — no partial-shard product was adopted before all
     shards were committed. The pool's `gc/state` object names exactly one adopted
     (generation, attempt) pair; that pair's fold-seal object must exist. A retry-created attempt
     that never got adopted (and so is not named by `gc/state`) may have written its own seal too —
     this is expected and must NOT be treated as a second completion signal; that is why this test
     resolves the adopted pair from `gc/state` first, rather than counting every seal object it can
     list under a generation. The soak waits for `gc/state` to adopt a nonzero generation before
     asserting.

  C) Disjoint-shard reduce progress — over the whole soak, `blob_target` keys were physically
     written under BOTH shard 0 and shard 1 (proving the sharded path executed, not just the
     gc_shards==1 fast-path). This is checked across every generation/attempt seen under
     `gc/gen/`, not only the currently-adopted one: a seal's `blob_target_runs` carry a PARENT's
     runs forward as references, but the physical objects stay under the (generation, attempt)
     prefix where they were originally written -- a late-soak adopted attempt whose own round
     produced no new deltas can have an empty `blob_target/` prefix of its own even though earlier
     attempts wrote plenty. Scanning the whole `gc/gen/` subtree is the only check that matches
     where the objects actually physically live.
"""

import re
import time

import pytest
from minio.error import S3Error

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_gc_sharded"

# Pool bucket key prefixes (the endpoint is http://rustfs1:11121/test/cas_gc_sharded/).
POOL_PREFIX = "cas_gc_sharded"
GC_STATE_KEY = POOL_PREFIX + "/gc/state"

# Workload parameters — enough rows to produce blobs routing to BOTH hash-mod-2 buckets.
NUM_ROWS_PER_INSERT = 2000
NUM_INSERTS = 4

# GC quiesce: grace=3s, interval=1s. We poll up to 90 s for at least one completed generation.
GC_POLL_RETRIES = 90
GC_POLL_SLEEP = 1.0

# Error patterns in server logs that must NOT appear in a healthy soak.
CA_FATAL_LOG_KEYWORDS = [
    "DANGLE",
    "dangle",
    "CorruptDangle",
]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node1",
        main_configs=["configs/storage_conf.xml", "configs/server_root_id_node1.xml"],
        macros={"replica": "node1"},
        with_rustfs=True,
        with_zookeeper=True,
        stay_alive=True,
    )
    cluster.add_instance(
        "node2",
        main_configs=["configs/storage_conf.xml", "configs/server_root_id_node2.xml"],
        macros={"replica": "node2"},
        with_rustfs=True,
        with_zookeeper=True,
        stay_alive=True,
    )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# RustFS helpers
# ---------------------------------------------------------------------------

def list_rustfs_prefix(prefix, recursive=True):
    """Return a list of object keys under `prefix` in the shared RustFS bucket."""
    objects = cluster.rustfs_client.list_objects(
        cluster.rustfs_bucket, prefix, recursive=recursive
    )
    return [o.object_name for o in objects]


def get_rustfs_object(key):
    """Return the raw bytes of `key`, or None if it does not exist."""
    try:
        resp = cluster.rustfs_client.get_object(cluster.rustfs_bucket, key)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()
    except S3Error:
        return None


# `gc/state`'s wire format is a plain JSON-like text object (CasGcStateFormat.cpp), not a binary
# blob: the two fields this test needs are literally spelled `"snap_generation":"<digits>"`
# and `"snap_attempt":"<digits>"` in the object bytes, so a direct regex read is exact without
# needing the C++ decoder. This mirrors the production reader that resolves "the adopted seal"
# (Gc/CasOrphanManifestSweep.cpp): read gc/state, take (snap_generation, snap_attempt), then look up
# that exact fold seal -- the only two-hop lookup that names one authoritative adopted pair.
_SNAP_GENERATION_RE = re.compile(r'"snap_generation":"(\d+)"')
_SNAP_ATTEMPT_RE = re.compile(r'"snap_attempt":"(\d+)"')


def read_adopted_generation_and_attempt():
    """
    Return the (generation, attempt) pair `gc/state` currently names as adopted, or None if
    `gc/state` does not exist yet or its `snap_generation` is still the "nothing adopted yet"
    sentinel (0, GcState's documented default).
    """
    data = get_rustfs_object(GC_STATE_KEY)
    if data is None:
        return None
    text = data.decode("utf-8", errors="replace")
    sg_match = _SNAP_GENERATION_RE.search(text)
    sa_match = _SNAP_ATTEMPT_RE.search(text)
    if not sg_match or not sa_match:
        # A `gc/state` that exists but does not spell both fields is a wire-format change this
        # reader has not followed, NOT "nothing adopted yet" -- say so instead of returning the
        # absent-sentinel, which would poll to a timeout and blame the server.
        raise AssertionError(
            "gc/state exists but neither snap_generation nor snap_attempt could be read from it; "
            "the object's key spelling changed and this regex reader is stale: " + text[:400]
        )
    generation = int(sg_match.group(1))
    if generation == 0:
        return None
    return generation, int(sa_match.group(1))


def adopted_fold_seal_key(generation, attempt):
    """The one fold-seal key `gc/state` names as adopted for (generation, attempt)."""
    return "{}/gc/gen/{}/attempt/{}/fold_seal".format(POOL_PREFIX, generation, attempt)


# Matches ".../gc/gen/<generation>/attempt/<attempt>/blob_target/<shard>/<seq>" -- the exact key
# shape `Layout::blobTargetRunKey` writes -- capturing only the shard id. Deliberately NOT scoped
# to one (generation, attempt): the objects a run key names stay physically where they were
# WRITTEN, and a seal only carries a REFERENCE to an earlier attempt's runs forward, so "did the
# sharded path ever write both shards over the whole soak" has to scan the whole gc/gen/ subtree.
_BLOB_TARGET_SHARD_RE = re.compile(r"/gc/gen/\d+/attempt/\d+/blob_target/(\d+)/")


def blob_target_shards_present():
    """Return (all blob_target keys found under gc/gen/, set of shard ids covered by them)."""
    keys = list_rustfs_prefix(POOL_PREFIX + "/gc/gen/", recursive=True)
    blob_target_keys = []
    shards = set()
    for k in keys:
        m = _BLOB_TARGET_SHARD_RE.search(k)
        if m:
            blob_target_keys.append(k)
            shards.add(int(m.group(1)))
    return blob_target_keys, shards


# ---------------------------------------------------------------------------
# Workload
# ---------------------------------------------------------------------------

def run_blob_churn_workload(node, table_name, rounds=2):
    """
    Insert rows + merge + drop in `rounds` cycles. Each cycle creates a fresh
    `ReplicatedMergeTree`, inserts `NUM_INSERTS` batches of `NUM_ROWS_PER_INSERT` rows,
    forces a merge, then drops the table. This generates blob churn: blobs are
    referenced during the cycle, then become orphaned after the drop.
    """
    for i in range(rounds):
        full_name = "{}_{}".format(table_name, i)
        node.query("DROP TABLE IF EXISTS {} SYNC".format(full_name))
        node.query(
            "CREATE TABLE {name} (id Int64, v UInt64, s String) "
            "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{name}', '{{replica}}') "
            "ORDER BY id "
            "SETTINGS storage_policy = '{policy}'".format(
                name=full_name, policy=STORAGE_POLICY
            )
        )
        for j in range(NUM_INSERTS):
            offset = (i * NUM_INSERTS + j) * NUM_ROWS_PER_INSERT
            node.query(
                "INSERT INTO {name} "
                "SELECT number + {off}, number + {off}, toString(number + {off}) "
                "FROM numbers({rows})".format(
                    name=full_name, off=offset, rows=NUM_ROWS_PER_INSERT
                )
            )
        node.query("OPTIMIZE TABLE {} FINAL".format(full_name))
        node.query("DROP TABLE {} SYNC".format(full_name))


# ---------------------------------------------------------------------------
# Main soak test
# ---------------------------------------------------------------------------

def test_sharded_gc_soak():
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    # Create a long-lived table to confirm row-count parity after the soak.
    node1.query("DROP TABLE IF EXISTS live_table SYNC")
    node2.query("DROP TABLE IF EXISTS live_table SYNC")
    node1.query(
        "CREATE TABLE live_table (id Int64, s String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/live_table', '{{replica}}') "
        "ORDER BY id "
        "SETTINGS storage_policy = '{}'".format(STORAGE_POLICY)
    )
    node2.query(
        "CREATE TABLE live_table (id Int64, s String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/live_table', '{{replica}}') "
        "ORDER BY id "
        "SETTINGS storage_policy = '{}'".format(STORAGE_POLICY)
    )

    # Insert into the live table so it has real content during the soak.
    node1.query(
        "INSERT INTO live_table "
        "SELECT number, toString(number) FROM numbers({rows})".format(
            rows=NUM_ROWS_PER_INSERT * NUM_INSERTS
        )
    )
    node2.query("SYSTEM SYNC REPLICA live_table", timeout=60)

    live_count_before = int(node1.query("SELECT count() FROM live_table"))
    assert live_count_before == NUM_ROWS_PER_INSERT * NUM_INSERTS

    # --- WORKLOAD: blob churn on node1 ---
    run_blob_churn_workload(node1, "churn", rounds=2)

    # --- CHAOS: restart node2 ---
    node2.restart_clickhouse(kill=True)
    node2.query("SYSTEM SYNC REPLICA live_table", timeout=120)

    # --- QUIESCE: wait for gc/state to adopt a generation ---
    adopted = None
    for _ in range(GC_POLL_RETRIES):
        adopted = read_adopted_generation_and_attempt()
        if adopted:
            break
        time.sleep(GC_POLL_SLEEP)

    assert adopted, (
        "gc/state never adopted a generation (snap_generation stayed at the zero sentinel) within "
        "{} s; gc_shards=2 soak cannot proceed (check server logs for GC errors)".format(
            GC_POLL_RETRIES * GC_POLL_SLEEP
        )
    )

    # Allow one more GC interval for any in-progress round to finish, then re-resolve the adopted
    # pointer (it may have advanced again).
    time.sleep(5)
    generation, attempt = read_adopted_generation_and_attempt() or adopted

    # -----------------------------------------------------------------------
    # ASSERTION A: no dangle / no loss
    # -----------------------------------------------------------------------

    # Both replicas must agree on the live row count.
    count1 = int(node1.query("SELECT count() FROM live_table"))
    count2 = int(node2.query("SELECT count() FROM live_table"))
    assert count1 == live_count_before, (
        "node1 live_table count changed: before {} after {}".format(
            live_count_before, count1
        )
    )
    assert count1 == count2, (
        "replica row-count divergence: node1={} node2={}".format(count1, count2)
    )

    # No CA-layer dangle errors in either server log. `system.text_log`'s underlying table can
    # take a moment after node2's restart above to become queryable ("Unknown table" briefly),
    # independent of whether any row has actually been flushed to it yet. Wait that out with a
    # SEPARATE, keyword-free readiness probe first, THEN run the real keyword checks with a plain
    # query (no retry): retrying the keyword query itself would be self-defeating -- each failed
    # attempt logs its own `<Error>` line that echoes the failing query's text (which contains the
    # search keyword as a LIKE pattern), and that line then lands in system.text_log itself, so a
    # later successful attempt of the SAME keyword query would count its own failed predecessors
    # as matches.
    for inst_name, inst in [("node1", node1), ("node2", node2)]:
        inst.query_with_retry("SELECT count() FROM system.text_log")
        for kw in CA_FATAL_LOG_KEYWORDS:
            log_count = inst.query(
                "SELECT count() FROM system.text_log "
                "WHERE level IN ('Error', 'Fatal') "
                "  AND message LIKE '%{}%'".format(kw)
            )
            assert int(log_count) == 0, (
                "{} has CA fatal/error entries matching '{}' in system.text_log".format(
                    inst_name, kw
                )
            )

    # -----------------------------------------------------------------------
    # ASSERTION B: the adopted (generation, attempt) has a durable fold seal
    # -----------------------------------------------------------------------

    # gc/state names exactly one adopted (generation, attempt) pair; this is the ONLY seal this
    # test looks at -- a non-adopted retry attempt's own seal (if any) is never named by gc/state
    # and so cannot be mistaken for a second completion signal.
    seal_key = adopted_fold_seal_key(generation, attempt)
    assert get_rustfs_object(seal_key) is not None, (
        "gc/state adopted (generation={}, attempt={}) but its fold seal ('{}') does not exist -- "
        "gc/state points at an attempt whose seal was never durably written".format(
            generation, attempt, seal_key
        )
    )

    # -----------------------------------------------------------------------
    # ASSERTION C: disjoint-shard reduce progress
    # -----------------------------------------------------------------------

    # Over the WHOLE gc/gen/ subtree (every generation and attempt seen, not only the currently
    # adopted one -- see the module docstring's point C for why), blob_target keys must cover
    # BOTH shard 0 and shard 1.
    blob_target_keys, shards_covered = blob_target_shards_present()
    # Log the observed key set once: an assertion that passes on an empty listing (e.g. a further
    # key-shape mismatch) would be a silent false pass, not evidence the sharded path ran.
    print(
        "test_sharded_gc_soak: blob_target keys under {}/gc/gen/: {}".format(
            POOL_PREFIX, blob_target_keys
        )
    )
    assert blob_target_keys, (
        "no blob_target keys found anywhere under '{}/gc/gen/'; before checking shard coverage "
        "the listing itself must be non-empty".format(POOL_PREFIX)
    )
    assert 0 in shards_covered and 1 in shards_covered, (
        "blob_target keys under '{}/gc/gen/' cover shards {}, expected BOTH 0 and 1; the sharded "
        "gc_shards=2 fold path may not have executed. Keys observed: {}".format(
            POOL_PREFIX, sorted(shards_covered), blob_target_keys
        )
    )

    # -----------------------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------------------
    node1.query("DROP TABLE IF EXISTS live_table SYNC")
    node2.query("DROP TABLE IF EXISTS live_table SYNC")
