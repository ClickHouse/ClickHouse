import re
import shlex
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Both replicas mount the SAME content-addressed pool (endpoint .../root/shared_pool/). A
# ReplicatedMergeTree part written on one replica is therefore ALREADY present (as content blobs +
# manifest) in the pool when the other replica needs it — so the "fetch" is a fetch-by-relink: the
# fetching replica publishes its own ref to the existing blobs instead of downloading any bytes (the CA
# analogue of zero-copy replication, spec §4).
STORAGE_POLICY = "cas_shared"
CA_DISK = "disk_cas_shared"

# A second, independent pool mounted by node2 only (configs/storage_conf_other_pool.xml). Used for the
# cross-pool leg of B66b: relink is gated on both sides naming the same pool, so a fetch into this one
# must degrade to bytes.
OTHER_STORAGE_POLICY = "cas_other"
OTHER_CA_DISK = "disk_cas_other"

# The shared pool's blob prefix inside the `test` RustFS bucket. The relink proof is that the fetch does
# NOT create new objects under here: relink publishes a ref (per-server, under store/), never a blob.
BLOBS_PREFIX = "shared_pool/blobs/"

NUM_ROWS = 10000

# ----------------------------------------------------------------------------------------------------
# WHY EVERY RELINK ASSERTION BELOW IS A POSITIVE ONE
#
# "The fetch created no new blobs" is NOT by itself evidence that a relink happened. On a
# content-addressed disk a BYTE fetch writes the very same content, which deduplicates against the
# blobs already in the pool, so its blob-count delta is zero too. A test that only counts blobs is
# therefore green whether the protocol worked or silently fell back — the single easiest worthless test
# on this path.
#
# So each relink test asserts a signal that is reachable ONLY through the intended path:
#
#   RELINK RAN   -> the receiver's `Relink of part <p> onto disk <d> finished (no bytes transferred).`
#                   That line is the last statement of `Fetcher::relinkPartToDisk` and is reachable only
#                   after the confirm answered `yes` AND `promote()` returned `Committed` (taxonomy
#                   row 4). Every other row returns or throws before it.
#   BYTES RAN    -> the receiver's `Download of part <p> onto disk <d> finished.` from
#                   `downloadPartToDisk`, plus the specific line naming WHY relink was declined.
#
# The blob-count / `CASBlobPut == 0` checks are kept as corroboration, never as the proof.
# ----------------------------------------------------------------------------------------------------


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
        main_configs=[
            "configs/storage_conf.xml",
            "configs/server_root_id_node2.xml",
            "configs/storage_conf_other_pool.xml",
        ],
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


def blob_keys():
    """Every object key under the shared pool's blob prefix, as a set."""
    objects = cluster.rustfs_client.list_objects(
        cluster.rustfs_bucket, BLOBS_PREFIX, recursive=True
    )
    return {obj.object_name for obj in objects}


def count_blobs():
    return len(blob_keys())


def log_lines(node, pattern):
    """Server-log lines matching an extended regular expression.

    Deliberately NOT `instance.grep_in_log`: that one globs `clickhouse-server.log*`, which includes
    `clickhouse-server.err.log`, so any warning-or-above line is counted twice. Several assertions here
    are exact counts, and a doubled count is indistinguishable from a real second attempt.
    """
    out = node.exec_in_container(
        [
            "bash",
            "-c",
            "grep -a -E {} /var/log/clickhouse-server/clickhouse-server.log || true".format(
                shlex.quote(pattern)
            ),
        ]
    )
    return [line for line in out.splitlines() if line.strip()]


def wait_for_log_lines(node, pattern, timeout=60):
    """Poll until at least one line matches, then return the matches. Fails loudly on timeout."""
    deadline = time.time() + timeout
    while True:
        found = log_lines(node, pattern)
        if found:
            return found
        assert time.time() < deadline, "timed out waiting for log lines matching {!r} on {}".format(
            pattern, node.name
        )
        time.sleep(0.5)


def relink_finished_pattern(table, part, disk=CA_DISK):
    """The receiver-side proof that the publish→confirm→promote path completed for this exact part."""
    return r"default\.{} .*Relink of part {} onto disk {} finished \(no bytes transferred\)".format(
        table, re.escape(part), disk
    )


def download_finished_pattern(table, part, disk=CA_DISK):
    """The receiver-side proof that the BYTE path completed for this exact part."""
    return r"default\.{} .*Download of part {} onto disk {} finished".format(
        table, re.escape(part), disk
    )


def relink_offer_pattern(table, part):
    """The SENDER-side line, one per relink offer actually made. The attempt counter."""
    return r"default\.{} .*Sending part {} by relink".format(table, re.escape(part))


def assert_relinked(node, table, part, disk=CA_DISK, timeout=60):
    wait_for_log_lines(node, relink_finished_pattern(table, part, disk), timeout=timeout)
    assert not log_lines(node, download_finished_pattern(table, part, disk)), (
        "part {} of {} was relinked AND byte-downloaded on {} — the relink proof is not exclusive".format(
            part, table, node.name
        )
    )


def assert_byte_downloaded(node, table, part, disk=CA_DISK, timeout=60):
    wait_for_log_lines(node, download_finished_pattern(table, part, disk), timeout=timeout)
    assert not log_lines(node, relink_finished_pattern(table, part, disk)), (
        "part {} of {} was expected to arrive as bytes but a relink completed on {}".format(
            part, table, node.name
        )
    )


def assert_no_new_blobs(before_keys):
    """Corroboration for a relink: the fetch added no object under the pool's blob prefix.

    Phrased as "no NEW key" rather than "the same count" on purpose — background GC may reclaim
    unrelated debris at any moment on this fixture (`gc_interval_sec` is 1), and a count that went DOWN
    says nothing about whether the fetch wrote anything.
    """
    new_keys = sorted(blob_keys() - before_keys)
    assert not new_keys, "the fetch wrote {} new blob(s), e.g. {}".format(len(new_keys), new_keys[:5])


def cas_blob_puts(node):
    return int(node.query("SELECT sum(value) FROM system.events WHERE event = 'CASBlobPut'") or 0)


def active_part_names(node, table):
    return node.query(
        "SELECT name FROM system.parts WHERE database = 'default' AND table = '{}' AND active "
        "ORDER BY name".format(table)
    ).split()


def any_state_part_count(node, table, part):
    return int(
        node.query(
            "SELECT count() FROM system.parts WHERE database = 'default' AND table = '{}' "
            "AND name = '{}'".format(table, part)
        )
    )


def wait_until(predicate, timeout, what):
    deadline = time.time() + timeout
    while True:
        if predicate():
            return
        assert time.time() < deadline, "timed out waiting for {}".format(what)
        time.sleep(0.5)


def fsck(node, disk=CA_DISK):
    """`SYSTEM CAS FSCK <disk>` as a dict of column -> value.

    Driven through `clickhouse-client --format` rather than a trailing `FORMAT` clause: `ASTSystemQuery`
    is not an `ASTQueryWithOutput`, so `SYSTEM ... FORMAT TSVWithNames` is a syntax error. Reading the
    header is what keeps this from depending on the column ORDER of the summary.
    """
    out = node.exec_in_container(
        [
            "bash",
            "-c",
            "clickhouse client --format TSVWithNames --query {}".format(
                shlex.quote("SYSTEM CAS FSCK '{}'".format(disk))
            ),
        ]
    ).splitlines()
    header, row = out[0].split("\t"), out[1].split("\t")
    summary = dict(zip(header, row))
    assert "dangling" in summary, "unexpected FSCK summary shape: {}".format(out)
    return summary


def gc_round(node, disk=CA_DISK):
    node.query("SYSTEM CAS GC RUN '{}'".format(disk))


def drop_everywhere(table):
    for node in (cluster.instances["node1"], cluster.instances["node2"]):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))


def create_replicated(node, table, policy=STORAGE_POLICY, zk_path=None, extra_settings=""):
    node.query(
        "CREATE TABLE {table} (id Int64, v UInt64, s String) "
        "ENGINE = ReplicatedMergeTree('{zk}', '{{replica}}') ORDER BY id "
        "SETTINGS storage_policy = '{policy}'{extra}".format(
            table=table,
            zk=zk_path or "/clickhouse/tables/" + table,
            policy=policy,
            extra=(", " + extra_settings) if extra_settings else "",
        )
    )


def insert_rows(node, table, start, rows=NUM_ROWS):
    node.query(
        "INSERT INTO {table} SELECT number, number * 10, toString(number) "
        "FROM numbers({start}, {rows})".format(table=table, start=start, rows=rows)
    )


def test_replicated_fetch_by_relink():
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS r SYNC")
    node2.query("DROP TABLE IF EXISTS r SYNC")

    # Two replicas of ONE ReplicatedMergeTree table on the shared CA pool. Lifting B33 is what makes this
    # CREATE succeed at all; the shared-pool mount is what makes the second replica start.
    create_tpl = (
        "CREATE TABLE r (id Int64, v UInt64, s String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/r', '{{replica}}') "
        "ORDER BY id SETTINGS storage_policy = '{policy}'"
    )
    node1.query(create_tpl.format(policy=STORAGE_POLICY))
    node2.query(create_tpl.format(policy=STORAGE_POLICY))

    # (1) INSERT on replica node1. node2 must replicate the part.
    node1.query(
        "INSERT INTO r SELECT number, number * 10, toString(number) FROM numbers({rows})".format(
            rows=NUM_ROWS
        )
    )

    # Blob count after the insert, BEFORE node2 fetches. This is the relink baseline.
    blobs_after_insert = count_blobs()
    assert blobs_after_insert > 0, "insert must have written content blobs to the shared pool"

    # (2) node2 fetches the part. SYNC REPLICA blocks until the queue (the fetch) drains.
    node2.query("SYSTEM SYNC REPLICA r", timeout=60)

    # (3) node2 reads the SAME rows back.
    expected_sum_id = (NUM_ROWS - 1) * NUM_ROWS // 2
    assert int(node2.query("SELECT count() FROM r")) == NUM_ROWS
    assert int(node2.query("SELECT sum(id) FROM r")) == expected_sum_id
    assert int(node2.query("SELECT sum(v) FROM r")) == expected_sum_id * 10

    # (4) THE RELINK PROOF: the fetch created NO new blob objects. node2 published a ref to the blobs
    #     node1 already wrote — it did not download/re-write them. (Relink, not byte download.)
    blobs_after_fetch = count_blobs()
    assert blobs_after_fetch == blobs_after_insert, (
        "fetch-by-relink must not create new blob objects: had {} after insert, {} after node2 fetched "
        "(a byte download would have re-written blobs)".format(
            blobs_after_insert, blobs_after_fetch
        )
    )

    # (5) A merge on node1 fetched-by-relink by node2: insert a second part on node1, OPTIMIZE to merge,
    #     and confirm node2 picks up the merged part with still no new blobs beyond the merge's own.
    node1.query(
        "INSERT INTO r SELECT number, number * 10, toString(number) FROM numbers({a}, {rows})".format(
            a=NUM_ROWS, rows=NUM_ROWS
        )
    )
    node2.query("SYSTEM SYNC REPLICA r", timeout=60)
    blobs_before_merge = count_blobs()

    node1.query("OPTIMIZE TABLE r FINAL")
    node1.query("SYSTEM SYNC REPLICA r", timeout=60)
    blobs_after_merge_on_node1 = count_blobs()

    # node2 fetches the merged part. The merge itself may write new blobs on node1 (the merged content),
    # but node2's FETCH of that merged part must add NOTHING further (relink).
    node2.query("SYSTEM SYNC REPLICA r", timeout=60)
    blobs_after_merge_fetch = count_blobs()
    assert blobs_after_merge_fetch == blobs_after_merge_on_node1, (
        "fetch-by-relink of the merged part must not create new blobs: {} after node1 merged, {} after "
        "node2 fetched".format(blobs_after_merge_on_node1, blobs_after_merge_fetch)
    )

    assert int(node2.query("SELECT count() FROM r")) == 2 * NUM_ROWS
    assert int(node1.query("SELECT count() FROM r")) == 2 * NUM_ROWS

    node1.query("DROP TABLE IF EXISTS r SYNC")
    node2.query("DROP TABLE IF EXISTS r SYNC")


def test_relink_happy_path_proof():
    """Task 16 step 2 — the happy path, proved POSITIVELY.

    Taxonomy row 4 (confirm `yes` -> `promote` -> `Committed`). The proof is the receiver's
    `... finished (no bytes transferred)` line, which no other row can reach; `CASBlobPut == 0` and the
    flat blob count are corroboration only (see the note at the top of this file).
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "relink_happy"
    drop_everywhere(table)

    create_replicated(node1, table)
    create_replicated(node2, table)

    node2.query("SYSTEM STOP FETCHES {}".format(table))
    insert_rows(node1, table, 0)
    part = active_part_names(node1, table)[0]

    blobs_before = blob_keys()
    puts_before = cas_blob_puts(node2)

    node2.query("SYSTEM START FETCHES {}".format(table))
    node2.query("SYSTEM SYNC REPLICA {}".format(table), timeout=60)

    # THE PROOF: reachable only after a confirm `yes` and a committed promote.
    assert_relinked(node2, table, part)

    # Corroboration, in the plan's own terms: the receiver issued no blob PUT at all, and the pool's
    # blob set is byte-identical to what the sender's insert left behind.
    assert cas_blob_puts(node2) == puts_before
    assert_no_new_blobs(blobs_before)

    assert int(node2.query("SELECT count() FROM {}".format(table))) == NUM_ROWS
    assert int(node2.query("SELECT sum(v) FROM {}".format(table))) == int(
        node1.query("SELECT sum(v) FROM {}".format(table))
    )

    drop_everywhere(table)


def test_fetch_part_into_detached_relinks():
    """Task 16 step 5 — B66b, manual caller #1: `ALTER TABLE ... FETCH PART ... FROM`.

    Taxonomy row 4 with `to_detached=true`: the staged ref is `detached/tmp-fetch_<part>` and the
    finalization is `renameTo(detached/<part>)`. Before B66b the relink capability was gated on
    `!to_detached`, so this fetch could only ever be bytes.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src, dst = "b66b_part_src", "b66b_part_dst"
    drop_everywhere(src)
    drop_everywhere(dst)

    create_replicated(node1, src)
    create_replicated(node2, dst)
    insert_rows(node1, src, 0)
    part = active_part_names(node1, src)[0]

    blobs_before = blob_keys()
    puts_before = cas_blob_puts(node2)

    node2.query(
        "ALTER TABLE {dst} FETCH PART '{part}' FROM '/clickhouse/tables/{src}'".format(
            dst=dst, part=part, src=src
        )
    )

    assert_relinked(node2, dst, part)
    assert cas_blob_puts(node2) == puts_before
    assert_no_new_blobs(blobs_before)

    # ... and the detached part is a real, readable part once attached.
    node2.query("ALTER TABLE {} ATTACH PART '{}'".format(dst, part))
    assert int(node2.query("SELECT count() FROM {}".format(dst))) == NUM_ROWS
    assert int(node2.query("SELECT sum(v) FROM {}".format(dst))) == int(
        node1.query("SELECT sum(v) FROM {}".format(src))
    )

    drop_everywhere(src)
    drop_everywhere(dst)


def test_fetch_partition_into_detached_relinks():
    """Task 16 step 5 — B66b, manual caller #2: `ALTER TABLE ... FETCH PARTITION ... FROM`.

    Same taxonomy row as the FETCH PART leg; a separate test because it is a separate call site (it
    fetches a whole partition through its own thread pool) and Task 15 changed both.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src, dst = "b66b_partition_src", "b66b_partition_dst"
    drop_everywhere(src)
    drop_everywhere(dst)

    create_replicated(node1, src)
    create_replicated(node2, dst)
    insert_rows(node1, src, 0)
    part = active_part_names(node1, src)[0]

    blobs_before = blob_keys()
    puts_before = cas_blob_puts(node2)

    node2.query(
        "ALTER TABLE {dst} FETCH PARTITION ID 'all' FROM '/clickhouse/tables/{src}'".format(
            dst=dst, src=src
        )
    )

    assert_relinked(node2, dst, part)
    assert cas_blob_puts(node2) == puts_before
    assert_no_new_blobs(blobs_before)

    node2.query("ALTER TABLE {} ATTACH PARTITION ID 'all'".format(dst))
    assert int(node2.query("SELECT count() FROM {}".format(dst))) == NUM_ROWS

    drop_everywhere(src)
    drop_everywhere(dst)


def test_detached_fetch_cross_pool_falls_back_to_bytes():
    """Task 16 step 5 — the cross-pool leg: relink is gated on ONE pool, so this must be bytes.

    Not a taxonomy row at all: the sender's pre-filter (`receiver_pool_uuid == getPoolUUID()`) declines
    to make an offer, so the receiver never enters `relinkPartToDisk`. The positive signal is therefore
    the byte path's own completion line plus the ABSENCE of any relink offer for this part.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src, dst = "xpool_src", "xpool_dst"
    drop_everywhere(src)
    drop_everywhere(dst)

    create_replicated(node1, src)
    create_replicated(node2, dst, policy=OTHER_STORAGE_POLICY)
    insert_rows(node1, src, 0)
    part = active_part_names(node1, src)[0]

    node2.query(
        "ALTER TABLE {dst} FETCH PART '{part}' FROM '/clickhouse/tables/{src}'".format(
            dst=dst, part=part, src=src
        )
    )

    # The bytes really moved: the receiver ran `downloadPartToDisk` onto the OTHER pool's disk.
    assert_byte_downloaded(node2, dst, part, disk=OTHER_CA_DISK)
    # ... and the sender never offered a relink for it, which is what makes the byte path the *intended*
    # outcome here rather than an accident of some later failure.
    assert not log_lines(node1, relink_offer_pattern(src, part))

    node2.query("ALTER TABLE {} ATTACH PART '{}'".format(dst, part))
    assert int(node2.query("SELECT count() FROM {}".format(dst))) == NUM_ROWS
    assert int(node2.query("SELECT sum(v) FROM {}".format(dst))) == int(
        node1.query("SELECT sum(v) FROM {}".format(src))
    )

    drop_everywhere(src)
    drop_everywhere(dst)


def test_attach_partition_from_relinks_on_queue_fetch():
    """Task 16 step 6 (RPL-5) — `ATTACH PARTITION ... FROM` replicates as `REPLACE_RANGE`.

    The source table exists only on node1, so node2 cannot clone locally and its queue entry falls
    through to `executeReplaceRange`'s `fetchSelectedPart` — a THIRD fetch call site, with its own
    `tmp_replace_from_fetch_` prefix. Taxonomy row 4; the proof is the same relink-finished line.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src, dst = "rpl5_attach_src", "rpl5_attach_dst"
    drop_everywhere(src)
    drop_everywhere(dst)

    node1.query(
        "CREATE TABLE {src} (id Int64, v UInt64, s String) ENGINE = MergeTree ORDER BY id "
        "SETTINGS storage_policy = '{policy}'".format(src=src, policy=STORAGE_POLICY)
    )
    create_replicated(node1, dst)
    create_replicated(node2, dst)
    insert_rows(node1, src, 0)

    node1.query("ALTER TABLE {dst} ATTACH PARTITION tuple() FROM {src}".format(dst=dst, src=src))
    part = active_part_names(node1, dst)[0]

    blobs_before = blob_keys()
    puts_before = cas_blob_puts(node2)

    node2.query("SYSTEM SYNC REPLICA {}".format(dst), timeout=90)

    assert_relinked(node2, dst, part)
    assert cas_blob_puts(node2) == puts_before
    assert_no_new_blobs(blobs_before)
    assert int(node2.query("SELECT count() FROM {}".format(dst))) == NUM_ROWS

    node1.query("DROP TABLE IF EXISTS {} SYNC".format(src))
    drop_everywhere(dst)


def test_replace_partition_relinks_on_queue_fetch():
    """Task 16 step 6 (RPL-5) — `REPLACE PARTITION`, i.e. the same entry with a drop range attached.

    Separate from the ATTACH leg because the destination is non-empty: node2 must drop its own covering
    part AND fetch the replacement, so the relink runs against a partition that already had a ref.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    src, dst = "rpl5_replace_src", "rpl5_replace_dst"
    drop_everywhere(src)
    drop_everywhere(dst)

    node1.query(
        "CREATE TABLE {src} (id Int64, v UInt64, s String) ENGINE = MergeTree ORDER BY id "
        "SETTINGS storage_policy = '{policy}'".format(src=src, policy=STORAGE_POLICY)
    )
    create_replicated(node1, dst)
    create_replicated(node2, dst)

    # Destination starts non-empty and replicated, so REPLACE really replaces something.
    insert_rows(node1, dst, 0)
    node2.query("SYSTEM SYNC REPLICA {}".format(dst), timeout=60)

    insert_rows(node1, src, 5 * NUM_ROWS)
    node1.query("ALTER TABLE {dst} REPLACE PARTITION tuple() FROM {src}".format(dst=dst, src=src))
    part = active_part_names(node1, dst)[0]

    node2.query("SYSTEM SYNC REPLICA {}".format(dst), timeout=90)

    assert_relinked(node2, dst, part)
    assert int(node2.query("SELECT count() FROM {}".format(dst))) == NUM_ROWS
    assert int(node2.query("SELECT sum(v) FROM {}".format(dst))) == int(
        node1.query("SELECT sum(v) FROM {}".format(dst))
    )

    node1.query("DROP TABLE IF EXISTS {} SYNC".format(src))
    drop_everywhere(dst)


def interserver_request(node, target_host, params):
    """One raw interserver request, straight at the sender's `DataPartsExchange` endpoint.

    The version-mix behaviour lives on the wire and nowhere else: which protocol version the peer
    advertises is not configurable, so the only way to exercise a NON-confirm-capable peer against this
    build's sender is to be that peer. Returns (headers, body_size).
    """
    query = "&".join("{}={}".format(k, v) for k, v in params)
    out = node.exec_in_container(
        [
            "bash",
            "-c",
            "curl -sS -o /tmp/ca_ism_body -D /tmp/ca_ism_hdr -w '%{{http_code}} %{{size_download}}' "
            "{url} >/tmp/ca_ism_stat; cat /tmp/ca_ism_hdr; echo '--STAT--'; cat /tmp/ca_ism_stat".format(
                url=shlex.quote("http://{}:9009/?{}".format(target_host, query))
            ),
        ]
    )
    headers, stat = out.split("--STAT--")
    http_code, size = stat.split()
    assert http_code == "200", "interserver request failed: {}\n{}".format(stat, headers)
    return headers, int(size)


def test_version_mix_legacy_peer_gets_bytes():
    """Task 16 step 7 — version mix: a peer that does not promise to confirm is served BYTES.

    This is the sender-side half of the mixed-build gate, and it is the half that is reachable without a
    second binary: the offer is gated on `client_protocol_version >= 11` (`..._WITH_CA_CONFIRM`), so a
    peer advertising 10 — a build that would relink WITHOUT confirming — must get the byte stream.
    Degrading to bytes, never to an unconfirmed relink, is the whole point of moving the gate to 11.

    The control request (identical, but advertising 11) is what makes the negative meaningful: it proves
    the request is otherwise perfectly relinkable, so the absence of an offer in the v10 case is the
    version gate and not a malformed request.

    The receiver-side row-1 branch — a genuinely OLD sender that offers a relink with NO source token —
    is NOT covered here; see the report accompanying this task.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "vermix"
    drop_everywhere(table)

    create_replicated(node1, table)
    create_replicated(node2, table)
    insert_rows(node1, table, 0)
    node2.query("SYSTEM SYNC REPLICA {}".format(table), timeout=60)
    part = active_part_names(node1, table)[0]

    # The pool identity as the SERVER reports it: taken from the sender's own offer line rather than
    # re-derived from the pool metadata, so the value fed back in is exactly what `getPoolUUID` returns.
    offers = wait_for_log_lines(node1, relink_offer_pattern(table, part))
    pool_uuid = re.search(r"shared pool ([0-9a-f]+)\)", offers[-1]).group(1)

    endpoint = "DataPartsExchange:/clickhouse/tables/{}/replicas/node1".format(table)
    base = [
        ("endpoint", endpoint),
        ("part", part),
        ("compress", "false"),
        ("cas_pool_uuid", pool_uuid),
    ]

    # CONTROL — a confirm-capable peer: an offer, with a token, and a tiny manifest-only body.
    headers_v11, size_v11 = interserver_request(
        node2, "node1", base + [("client_protocol_version", "11")]
    )
    assert "cas_relink=part_manifest_v2" in headers_v11, headers_v11
    assert "cas_source_token=" in headers_v11, headers_v11

    # THE CASE UNDER TEST — a peer advertising the pre-confirm version: no offer, and the part's bytes.
    headers_v10, size_v10 = interserver_request(
        node2, "node1", base + [("client_protocol_version", "10")]
    )
    assert "cas_relink" not in headers_v10, headers_v10
    assert "cas_source_token" not in headers_v10, headers_v10
    assert "server_protocol_version=10" in headers_v10, headers_v10

    # Positive proof that bytes ACTUALLY moved rather than the request merely succeeding: the v10
    # response carries the whole part, orders of magnitude more than the manifest-only relink payload.
    assert size_v10 > 20 * size_v11, (
        "the v10 peer should have received the part's bytes, got {} bytes against the relink offer's "
        "{}".format(size_v10, size_v11)
    )

    drop_everywhere(table)


def test_recursion_brake_bounds_relink_to_one_attempt():
    """Task 16 step 4 — the `allow_ca_relink` recursion brake.

    A mechanism failure that is a property of the sender/receiver PAIR reproduces on every attempt, so
    without the brake the byte-fetch fallback re-advertises the pool, is re-offered a relink, fails
    again, and recurses until the stack is gone. The failpoint injects exactly that class of failure
    (taxonomy rows 2 and 5 share this ACTION), because no configuration can produce one.

    The assertion is a COUNT, not termination: exactly ONE relink offer is made for this part, and then
    the bytes arrive. Termination alone would also hold for a brake that merely bounded the recursion at
    some larger depth, and it would hold vacuously if the relink path were never entered at all.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "brake"
    drop_everywhere(table)

    create_replicated(node1, table)
    create_replicated(node2, table)

    node2.query("SYSTEM STOP FETCHES {}".format(table))
    insert_rows(node1, table, 0)
    part = active_part_names(node1, table)[0]

    node2.query("SYSTEM ENABLE FAILPOINT cas_relink_receiver_force_mechanism_failure")
    try:
        node2.query("SYSTEM START FETCHES {}".format(table))
        node2.query("SYSTEM SYNC REPLICA {}".format(table), timeout=90)

        # The receiver hit the injected failure exactly once...
        hits = log_lines(
            node2,
            r"Failpoint cas_relink_receiver_force_mechanism_failure: abandoning the relink of part {}".format(
                re.escape(part)
            ),
        )
        assert len(hits) == 1, "expected exactly one relink attempt, got {}:\n{}".format(
            len(hits), "\n".join(hits)
        )

        # ... and the SENDER, independently, made exactly one offer. This is the sharper of the two: the
        # re-request is what would re-open the capability, and the sender is the only party that can say
        # whether it did.
        offers = log_lines(node1, relink_offer_pattern(table, part))
        assert len(offers) == 1, "expected exactly one relink offer, got {}:\n{}".format(
            len(offers), "\n".join(offers)
        )

        # And the fetch still succeeded, over the byte path.
        assert_byte_downloaded(node2, table, part)
        assert int(node2.query("SELECT count() FROM {}".format(table))) == NUM_ROWS
        assert int(node2.query("SELECT sum(v) FROM {}".format(table))) == int(
            node1.query("SELECT sum(v) FROM {}".format(table))
        )
    finally:
        node2.query("SYSTEM DISABLE FAILPOINT cas_relink_receiver_force_mechanism_failure")

    drop_everywhere(table)


# Settings that make node1 drop an outdated part — and with it the CA ref the confirm asks about —
# within a few seconds instead of the default eight minutes.
FAST_OLD_PART_REMOVAL = (
    "old_parts_lifetime = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1, "
    "max_cleanup_delay_period = 1"
)


def open_publish_confirm_window(node1, node2, table, base):
    """Drive a relink up to the paused point BETWEEN the receiver's durable `+1` and the confirm.

    Returns `(part, part_blobs)`: the name of the part whose relink is now stalled, and the blob keys
    that its insert ADDED to the pool. The delta matters — debris from earlier tests in this module may
    still be sitting in the pool and may legitimately be reclaimed while the window is open, so only the
    keys this part created can be asserted about. The caller MUST resume the failpoint.

    `base` shifts the generated rows so this table's column data is unlike any other table's in this
    module. Without it the content-addressed store deduplicates the insert against an earlier test's
    identical blobs and the delta is EMPTY — which would make every blob assertion below vacuous.
    """
    create_replicated(node1, table, extra_settings=FAST_OLD_PART_REMOVAL)
    create_replicated(node2, table, extra_settings=FAST_OLD_PART_REMOVAL)

    node2.query("SYSTEM STOP FETCHES {}".format(table))
    before_insert = blob_keys()
    insert_rows(node1, table, base)
    part = active_part_names(node1, table)[0]
    part_blobs = blob_keys() - before_insert
    assert part_blobs, "the insert wrote no new blob into the shared pool"

    node2.query("SYSTEM ENABLE FAILPOINT cas_relink_receiver_pause_before_confirm")
    node2.query("SYSTEM START FETCHES {}".format(table))
    # Blocks until the fetch thread is parked inside `relinkPartToDisk`, after `prepareAdoptFromManifest`
    # made the receiver's `+1` durable and before the confirm request is built.
    node2.query("SYSTEM WAIT FAILPOINT cas_relink_receiver_pause_before_confirm PAUSE", timeout=120)
    return part, part_blobs


def merge_the_source_part_away(node1, table, part, base):
    """While the receiver is parked: make the sender stop holding the exact binding it offered."""
    insert_rows(node1, table, base + NUM_ROWS)
    node1.query("OPTIMIZE TABLE {} FINAL".format(table))
    # The confirm is answered from the sender's live state, so the test is only meaningful once the old
    # part — and the ref naming its manifest — is really gone, not merely Outdated.
    wait_until(
        lambda: any_state_part_count(node1, table, part) == 0,
        timeout=120,
        what="node1 to drop the outdated part {}".format(part),
    )


def test_confirm_refuses_when_source_dropped_in_window():
    """Task 16 step 1 — the race the confirm exists to lose safely.

    Taxonomy row 3: the source cannot prove it still holds the offered manifest, so the receiver aborts
    its durable `+1` and throws a retry-later `NETWORK_ERROR` INSTEAD of falling back to bytes. The two
    assertions that matter are (a) the queue recovers by re-selecting — here, onto the covering part —
    and (b) NO byte re-request ever went to the source whose state was in doubt. (b) is the entire
    reason row 3 throws where rows 2 and 5 return `nullptr`.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "race_confirm"
    drop_everywhere(table)

    try:
        part, _ = open_publish_confirm_window(node1, node2, table, base=1_000_000)
        merge_the_source_part_away(node1, table, part, base=1_000_000)
    finally:
        node2.query("SYSTEM DISABLE FAILPOINT cas_relink_receiver_pause_before_confirm")

    # POSITIVE SIGNAL for row 3: the locally generated refusal, naming the source and the part.
    wait_for_log_lines(
        node2,
        r"Source .* did not prove it still holds the manifest it offered for part {}".format(
            re.escape(part)
        ),
        timeout=120,
    )

    # (a) the queue re-selects rather than losing the data.
    node2.query("SYSTEM SYNC REPLICA {}".format(table), timeout=180)
    assert int(node2.query("SELECT count() FROM {}".format(table))) == 2 * NUM_ROWS
    assert int(node2.query("SELECT sum(v) FROM {}".format(table))) == int(
        node1.query("SELECT sum(v) FROM {}".format(table))
    )
    assert active_part_names(node2, table) == active_part_names(node1, table)

    # (b) the abandoned part was never re-requested as bytes from the same source, and it was never
    #     promoted either — both would be a violation of the row-3 contract.
    assert not log_lines(node2, download_finished_pattern(table, part)), (
        "row 3 must not fall back to a byte re-request against the source it could not confirm"
    )
    assert not log_lines(node2, relink_finished_pattern(table, part))
    assert any_state_part_count(node2, table, part) == 0

    drop_everywhere(table)


def test_stalled_publish_protects_source_blobs_and_commits_nothing():
    """Task 16 step 3 — the codex-6 regression, which is why publish-then-confirm exists at all.

    The receiver's `+1` is durable while the fetch is stalled. Across the stall the sender merges the
    part away and GC runs to a fixpoint several times over: the offered manifest's blobs MUST survive,
    because the stalled receiver's own binding protects them — that is what makes the later confirm a
    meaningful question rather than a race against a sweep. And when the confirm finally answers
    `unproven`, the stalled attempt must leave NOTHING committed.

    The soundness guard is the last assertion: once the attempt is abandoned and the sender no longer
    holds the part, GC DOES reclaim those same blobs. Without it, "the blobs survived" would also be
    satisfied by a GC that never deletes anything.
    """
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "codex6_stall"
    drop_everywhere(table)

    try:
        # `part_blobs` is exactly what this part's insert added to the pool — see the helper for why it
        # has to be the delta and not everything under the prefix.
        part, part_blobs = open_publish_confirm_window(node1, node2, table, base=2_000_000)

        merge_the_source_part_away(node1, table, part, base=2_000_000)

        # Four full GC rounds on both mounters, spread well past the pool's 3-second condemn grace, so
        # a blob that was NOT protected would have been condemned, aged out and deleted in the window.
        for _ in range(4):
            gc_round(node1)
            gc_round(node2)
            time.sleep(1.5)

        missing = sorted(part_blobs - blob_keys())
        assert not missing, (
            "the stalled receiver's durable +1 must protect the offered manifest's blobs across GC; "
            "{} of {} were reclaimed, e.g. {}".format(len(missing), len(part_blobs), missing[:5])
        )
    finally:
        node2.query("SYSTEM DISABLE FAILPOINT cas_relink_receiver_pause_before_confirm")

    wait_for_log_lines(
        node2,
        r"Source .* did not prove it still holds the manifest it offered for part {}".format(
            re.escape(part)
        ),
        timeout=120,
    )

    # Nothing was committed by the stalled attempt.
    assert any_state_part_count(node2, table, part) == 0
    assert not log_lines(node2, relink_finished_pattern(table, part))

    node2.query("SYSTEM SYNC REPLICA {}".format(table), timeout=180)
    assert int(node2.query("SELECT count() FROM {}".format(table))) == 2 * NUM_ROWS

    # No dangling reference anywhere in the pool, from either mounter's point of view.
    for node in (node1, node2):
        summary = fsck(node)
        assert summary["dangling"] == "0", "{} fsck: {}".format(node.name, summary)

    # THE SOUNDNESS GUARD, and it is what makes the survival asserted earlier mean anything: with the
    # part gone from both replicas and the stalled attempt abandoned, its unique blobs are unreachable,
    # so GC reclaiming them proves their survival DURING the stall was the relink pin and not GC
    # inactivity. Without this, "the blobs were still there" would also be what a GC that never ran
    # produces.
    reclaimed = set()
    for _ in range(8):
        gc_round(node1)
        gc_round(node2)
        reclaimed = part_blobs - blob_keys()
        if reclaimed == part_blobs:
            break
    # Pool-wide: the GC lease is held by ONE server and it need not be node1.
    rounds = 0
    for n in (node1, node2):
        n.query("SYSTEM FLUSH LOGS")
        rounds += int(
            n.query(
                "SELECT count() FROM system.cas_gc_log "
                "WHERE event_type = 'Finish' AND outcome = 'Success'"
            ).strip()
            or 0
        )
    assert rounds > 0, "no successful GC round ran at all"
    assert reclaimed, (
        "none of the abandoned attempt's {} blob(s) were reclaimed, so their survival during the "
        "stall does not distinguish the relink pin from an inactive GC".format(len(part_blobs))
    )

    drop_everywhere(table)
