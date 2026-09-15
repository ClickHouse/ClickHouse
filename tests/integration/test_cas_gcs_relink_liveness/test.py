"""Fetch-by-relink liveness on a slow control plane.

Two replicas of one ReplicatedMergeTree table share one CAS pool over the fake GCS service of
`test_cas_gcs`, with every write to a key containing `_ckpt` delayed. That substring targets the
ref-lane flush's checkpoint publication, but it also matches namespace creation, recovery, and the GC
snapshot publisher's checkpoint contribution — all four are slowed, not just the flush. Both replicas
insert continuously, so each is a sender and a receiver at once and each keeps its own lane busy. A
confirm rule that refuses whenever the sender's lane is busy starves both replication queues here
(finding F11 of the 2026-09-02 live GCS campaign); the ref-scoped rule lets them drain. The unit tests
pin the rule; this is the liveness reproduction they cannot give.
"""
import json
import os
import shlex
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

GCS_HOST = "fakegcs"
GCS_PORT = 8080
CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/cas_gcs_shared.xml"
MOCK_DIR = os.path.join(os.path.dirname(__file__), "..", "test_cas_gcs", "gcs_mocks")
NODES = ("node1", "node2")
CA_DISK = "disk_cas_gcs_shared"

# Every `_ckpt` PUT sleeps this long: one second is the real service's own per-object mutation cap
# (see the module docstring above). A full run at this value takes several minutes.
CKPT_DELAY_MS = 1000
INSERTS_PER_NODE = 80
ROWS_PER_INSERT = 1000
DRAIN_TIMEOUT_S = 180

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    # As in test_cas_gcs: a CAS disk mounts at server start and fails closed when its store is
    # unreachable, and the fake can only be launched once its container is up. So both nodes start
    # without the disk, the disk configuration is installed with the node's own server root, and each
    # node is restarted once the fake answers.
    for name in NODES:
        cluster.add_instance(name, macros={"replica": name}, with_zookeeper=True, stay_alive=True)
    cluster.add_instance(
        GCS_HOST, hostname=GCS_HOST, image="altinityinfra/python-bottle", tag="latest", stay_alive=True
    )
    try:
        cluster.start()
        start_mock_servers(cluster, MOCK_DIR, [("server.py", GCS_HOST, str(GCS_PORT))])
        for name in NODES:
            node = cluster.instances[name]
            node.copy_file_to_container(
                os.path.join(os.path.dirname(__file__), "configs", "storage_conf.xml"),
                CONFIG_IN_CONTAINER,
            )
            node.replace_in_config(CONFIG_IN_CONTAINER, "__SERVER_ROOT_ID__", name)
            node.restart_clickhouse()
        yield cluster
    finally:
        cluster.shutdown()


def _control_post(path):
    container = cluster.get_container_id(GCS_HOST)
    return cluster.exec_in_container(
        container, ["curl", "-sS", "-X", "POST", "http://localhost:{}{}".format(GCS_PORT, path)]
    )


def _control_get(path):
    container = cluster.get_container_id(GCS_HOST)
    return cluster.exec_in_container(
        container, ["curl", "-sS", "http://localhost:{}{}".format(GCS_PORT, path)]
    )


def _set_delay(substr, ms):
    # `curl -sS` exits 0 on an HTTP 404 or 500, and a docker-exec only raises on a non-zero exit
    # status — so a renamed endpoint or a renamed query parameter would otherwise go unnoticed here
    # and the rest of the test would exercise a fake running at full speed. Checking the echoed body
    # is what makes a broken lever fail loudly instead of silently.
    reply = _control_post("/_control/delay?substr={}&ms={}".format(substr, ms))
    echoed = json.loads(reply)
    assert {"substr": substr, "ms": ms}.items() <= echoed.items(), (
        "fake did not echo back the delay setting it was asked for: {!r}".format(reply)
    )


def _delayed_put_count():
    counters = json.loads(_control_get("/_control/counters"))
    return counters.get("DelayedPut", 0)


def _queue_size(node, table):
    return int(
        node.query("SELECT count() FROM system.replication_queue WHERE table = '{}'".format(table))
    )


def _queue_breakdown(node, table):
    # What a bare queue-size number cannot say: WHICH kind of entry is stuck and why. A future stuck
    # run for an unrelated reason (a merge stall, a ZooKeeper hiccup) would otherwise print a
    # byte-identical failure message to this test's own liveness symptom.
    return node.query(
        "SELECT type, count(), any(last_exception) FROM system.replication_queue "
        "WHERE table = '{}' GROUP BY type ORDER BY type FORMAT TSV".format(table)
    )


def _replica_status(node, table):
    row = node.query(
        "SELECT queue_size, absolute_delay, log_pointer, log_max_index "
        "FROM system.replicas WHERE table = '{}' FORMAT TSV".format(table)
    )
    queue_size, absolute_delay, log_pointer, log_max_index = row.split()
    return int(queue_size), int(absolute_delay), int(log_pointer), int(log_max_index)


def _drained(node, table):
    # `queue_size == 0` alone is checked right after the insert threads join, before the
    # queue-updating thread is guaranteed to have pulled the peer's latest log entries — so a node
    # can read an empty queue with fetches still outstanding, and closing the delay window in that
    # instant would let the tail race to completion at full speed and pass for the wrong reason.
    # `log_pointer > log_max_index` (every log entry has been copied into the execution queue) and
    # `absolute_delay == 0` together rule that race out.
    queue_size, absolute_delay, log_pointer, log_max_index = _replica_status(node, table)
    return queue_size == 0 and absolute_delay == 0 and log_pointer > log_max_index


def _refusal_counters(node):
    return node.query(
        "SELECT event, value FROM system.events WHERE event LIKE 'CASRelinkConfirmRefused%' "
        "ORDER BY event FORMAT TSV"
    )


def _refusal_counter(node, event):
    # `system.events` has no row for an event that never fired, so an empty result is a zero, not an
    # error — which is what lets the caller assert a clean zero rather than having to special-case a
    # missing row. A misspelled event name reads the same way, so a zero here is never by itself
    # evidence that the named counter exists.
    value = node.query(
        "SELECT value FROM system.events WHERE event = '{}'".format(event)
    ).strip()
    return int(value) if value else 0


def _log_lines(node, pattern):
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


def _relink_finished_pattern(table, disk):
    """The receiver-side proof that a fetch completed by relink, not by byte transfer.

    Reachable only after `Fetcher::relinkPartToDisk`'s confirm step answered yes and `promote()`
    returned `Committed` — every other row in that function returns or throws before this line, so its
    presence cannot be produced by a fallback to bytes. Same pattern as
    `test_cas_replicated_relink.relink_finished_pattern`, generalised to any part name since this test
    does not track individual part names.
    """
    return r"default\.{} .*Relink of part .* onto disk {} finished \(no bytes transferred\)".format(
        table, disk
    )


def test_both_queues_drain_under_slow_checkpoints():
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    table = "relink_liveness"
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
        node.query(
            "CREATE TABLE {t} (id Int64, v UInt64) "
            "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{t}', '{{replica}}') "
            "ORDER BY id SETTINGS storage_policy = 'cas_gcs_shared'".format(t=table)
        )

    _set_delay("_ckpt", CKPT_DELAY_MS)
    try:
        errors = []

        def insert_loop(node, base):
            try:
                for i in range(INSERTS_PER_NODE):
                    node.query(
                        "INSERT INTO {} SELECT number, number * 10 FROM numbers({}, {})".format(
                            table, base + i * ROWS_PER_INSERT, ROWS_PER_INSERT
                        )
                    )
            except Exception as e:  # surfaced below, on the test thread
                errors.append((node.name, repr(e)))

        threads = [
            threading.Thread(target=insert_loop, args=(node1, 0)),
            threading.Thread(target=insert_loop, args=(node2, 10_000_000)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == [], errors

        # The lever, checked BEFORE the drain assertion: if the delay knob silently stopped
        # matching (a renamed endpoint, a renamed query param, a `_ckpt` key that stopped matching
        # the substring), the fake would serve every write at full speed and the drain assertion
        # below would pass having exercised nothing. Checking this first means a failure here is
        # never confused with the liveness failure this test exists to catch.
        delayed = _delayed_put_count()
        assert delayed >= 2 * INSERTS_PER_NODE, (
            "the delay knob fired only {} times; expected at least {} — it may have silently "
            "stopped matching `_ckpt` PUTs, which would make any drain result meaningless".format(
                delayed, 2 * INSERTS_PER_NODE
            )
        )

        # Liveness: the insert threads have already joined, so each replica's lane is kept busy from
        # here on by its own fetch bookkeeping alone — the self-sustaining half of the livelock. Both
        # queues must still drain.
        deadline = time.time() + DRAIN_TIMEOUT_S
        while time.time() < deadline and not (_drained(node1, table) and _drained(node2, table)):
            time.sleep(1)
        drained = (_drained(node1, table), _drained(node2, table))
        sizes = (_queue_size(node1, table), _queue_size(node2, table))
        for node in (node1, node2):
            print(node.name, "refusal counters:", _refusal_counters(node))
        assert drained == (True, True), (
            "replication queues did not drain in {} s with slow checkpoints: node1={} node2={}\n"
            "node1 queue (type, count, last_exception):\n{}\n"
            "node2 queue (type, count, last_exception):\n{}".format(
                DRAIN_TIMEOUT_S,
                sizes[0],
                sizes[1],
                _queue_breakdown(node1, table),
                _queue_breakdown(node2, table),
            )
        )

        # Transport proof, checked AFTER the drain assertion on purpose: under the old rule the
        # parts never arrive at all, so a relink assertion placed before the drain check would fail
        # for the second-best reason and muddy the evidence this test exists to produce. "Both
        # queues drained" cannot by itself distinguish a relinked fetch from a byte fetch that
        # dropped into one of `relinkPartToDisk`'s silent fallback exits — this line is reachable
        # only through the intended path.
        for node in (node1, node2):
            finished = _log_lines(node, _relink_finished_pattern(table, CA_DISK))
            assert finished, (
                "{} drained its queue without a single relink completing — every part that "
                "arrived took some route other than fetch-by-relink".format(node.name)
            )

        # No confirm may be refused by lane STATE on a healthy run. These two counters move only on
        # `confirmExactRef`'s wedge and broken-lane branches — an unresolved append, or a lane in
        # NeedsRecovery, Closed, Faulted, or Writing with nothing carved. This case drives the fake's
        # control plane with a delay and never with a fault, so none of those is reachable here and a
        # non-zero value is a lane defect the drain assertion above can hide: a confirm refused
        # table-wide costs the receiver only a retry, and enough retries still finish inside the drain
        # window. Against a real bucket a wedge IS reachable from load alone — sustained throttling can
        # exhaust an append's retry budget and leave its outcome unresolved — so this assertion rests
        # on the fault-free stand and would have to be rethought before it ran anywhere else.
        #
        # A zero here is not evidence that the counter exists: `_refusal_counter` reads a missing row
        # as a zero, and a misspelled or unregistered name reads the same way.
        #
        # What is NOT asserted, deliberately: `CASRelinkConfirmRefusedRefMutationInFlight` above zero.
        # A refusal there needs a queued or carved mutation naming the very ref the peer is asking
        # about, and each node's lane is busy with its own newer parts rather than with the seconds-old
        # part its peer is fetching — a run that passed both assertions above left node1 with no
        # `CASRelinkConfirmRefused%` row at all. Requiring it would demand the symptom the ref-scoped
        # rule removes, so it would pass only while the livelock is present. The two things it was
        # meant to show are shown better above: the relink-completion assertion proves confirms were
        # asked and answered `Yes`, and the delayed-PUT floor proves the checkpoint publications were
        # slowed, which is the contention itself. Its attribution is pinned in the unit suite, by
        # `CASConfirmExactRef.UntouchedRefConfirmsWhileAnotherRefIsQueued`.
        for node in (node1, node2):
            for event in ("CASRelinkConfirmRefusedLaneWedged", "CASRelinkConfirmRefusedLaneBroken"):
                assert _refusal_counter(node, event) == 0, (
                    "{} refused a relink confirm by lane state ({}), which no fault was injected to "
                    "produce. All refusal counters on this node:\n{}".format(
                        node.name, event, _refusal_counters(node)
                    )
                )
    finally:
        try:
            _set_delay("", 0)
        except Exception as exc:
            # Never let a failure here mask a real assertion failure raised above.
            print("failed to clear the delay knob:", repr(exc))

    expected = 2 * INSERTS_PER_NODE * ROWS_PER_INSERT
    assert int(node1.query("SELECT count() FROM {}".format(table))) == expected
    assert int(node2.query("SELECT count() FROM {}".format(table))) == expected
    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
