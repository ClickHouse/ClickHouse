#!/usr/bin/env python3
"""
Regression test: a Keeper node restarted with log entries that still have to be replayed
must keep its Raft event loop running while it replays them.

`KeeperServer::callbackFunc` used to park the calling thread in
`KeeperContext::waitLocalLogsPreprocessedOrShutdown` - a wait without a deadline - for every
`append_entries` request that carries entries and arrives before the replay is over. That
callback runs on a thread of the Raft event loop, which also runs the listener, the election
and heartbeat timers and every RPC completion, and the leader force-reconnects a peer that does
not answer, so one more thread was consumed per reconnect until the node stopped being a Raft
participant altogether while still looking alive to everything else.

The node here gets a replay that lasts seconds and a leader that force-reconnects every 100 ms,
which is the same situation with the time axis compressed. Two things are checked: the leader
backs off instead of resending entries the node cannot take, and the number of threads waiting
at the same time - reconstructed from the paired log lines - never exceeds one.

The second test covers the case where the leader may *not* be asked to back off, because the
restarted node has a tail that the leader does not have and can only learn where the two logs
match from a request that carries entries.
"""

import logging
import re
import time
from multiprocessing.dummy import Pool

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)

ALL_NODES = [node1, node2, node3]

# One transaction is one log entry, so the replay is 150 entries that cost 150000 znodes,
# applied twice: preprocessed and committed. That is seconds of replay out of a handful of
# entries, without writing a huge changelog.
TRANSACTIONS = 150
CREATES_PER_TRANSACTION = 1000

WAIT_STARTED = "ProcessReq callback: waiting for preprocessing"
WAIT_STOPPED = "ProcessReq callback: stopped waiting for preprocessing"
ENTRIES_REFUSED = "Logs not preprocessed, ProcessReq callback with"
NO_REPLAY_NEEDED = "No log preprocessing needed"

LOG_LINE = re.compile(r"^\S+ (\S+) \[ (\d+) \]")
TAIL_CORRECTION = re.compile(
    r"GotAppendEntryReqFromLeader callback with last_log_idx=(\d+), "
    r"current last_log_idx_on_disk=(\d+)"
)
SERVER_LOG = "/var/log/clickhouse-server/clickhouse-server.log"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_fake_zk(node, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, node.name, timeout=timeout)


def start_and_connect(node):
    node.start_clickhouse(start_wait_sec=240)
    keeper_utils.wait_until_connected(cluster, node, timeout=240)


def grep_log(node, pattern):
    """Lines matching `pattern` that the node logged since it was last started.

    Every integration test runs with `rotateOnOpen`, so a start rotates the previous
    `clickhouse-server.log` away and opens an empty one: the current file holds exactly the run
    that is being looked at, and the whole of it. Addressing it by a line number taken before
    the restart would point past the end of the new, shorter file and quietly match nothing.
    """
    output = node.exec_in_container(
        ["bash", "-c", f"grep -E -- '{pattern}' {SERVER_LOG}; true"]
    )
    return [line for line in output.splitlines() if line]


def waiting_thread_events(node):
    """(+1 started / -1 stopped) events, in log order, for the preprocessing wait."""
    events = []
    for line in grep_log(node, f"{WAIT_STARTED}|{WAIT_STOPPED}"):
        match = LOG_LINE.match(line)
        if match is None:
            continue
        events.append((match.group(1), match.group(2), -1 if WAIT_STOPPED in line else +1))
    return events


def test_raft_event_loop_is_not_wedged_by_log_replay(started_cluster):
    keeper_utils.wait_nodes(cluster, ALL_NODES)

    # 1) Build the tail. Nobody takes a snapshot, so after a restart node2 has to replay it.
    zk = get_fake_zk(node1)
    try:
        zk.create("/bulk")
        for transaction in range(TRANSACTIONS):
            request = zk.transaction()
            for i in range(CREATES_PER_TRANSACTION):
                request.create(f"/bulk/n{transaction:05d}_{i:05d}", b"")
            request.commit()
    finally:
        zk.stop()
        zk.close()

    # 2) The whole tail has to be in node2's own changelog, not only in the quorum, otherwise
    #    there is nothing for it to replay. Ask node2 itself for the last znode written.
    last_znode = f"/bulk/n{TRANSACTIONS - 1:05d}_{CREATES_PER_TRANSACTION - 1:05d}"
    zk = get_fake_zk(node2)
    try:
        for _ in range(120):
            if zk.exists(last_znode) is not None:
                break
            time.sleep(0.5)
        else:
            raise Exception("node2 did not receive the whole tail")
    finally:
        zk.stop()
        zk.close()

    # 3) Kill node2: SIGKILL leaves no shutdown snapshot behind, so its state machine restarts
    #    from nothing while its changelog holds the whole tail.
    node2.stop_clickhouse(kill=True)

    # 4) Put the leader ahead of node2's changelog, so that after the restart it has real entries
    #    to send and keeps re-sending them for as long as node2 refuses them.
    zk = get_fake_zk(node1)
    try:
        for i in range(10):
            zk.create(f"/ahead_of_node2_{i}", b"")
    finally:
        zk.stop()
        zk.close()

    # 5) Restart node2 and let it replay. Without the fix, its Raft event loop is dead once the
    #    reconnects have consumed every thread of the pool, and only comes back when the replay
    #    finishes on the commit thread, which is not part of that pool.
    node2.start_clickhouse(start_wait_sec=240)
    keeper_utils.wait_until_connected(cluster, node2, timeout=240)

    # The precondition of the bug has to have been reached: node2 must have restarted with log
    # entries that were not covered by a snapshot, otherwise nothing below checks anything.
    assert not grep_log(node2, NO_REPLAY_NEEDED), (
        "node2 restarted with nothing to replay, so this test checks nothing"
    )

    # node2 must have refused at least one request carrying entries, otherwise the leader never
    # reached it while it was replaying and the assertions below are vacuous. It must also have
    # refused only a handful: once it knows it can finish the replay on its own it asks the
    # leader to pause, so the count stays flat instead of growing with the length of the replay.
    refused = grep_log(node2, ENTRIES_REFUSED)
    logging.info("node2 refused the entries of %s append_entries requests", len(refused))
    assert refused, "the leader never sent entries to node2 while it was replaying"
    assert len(refused) <= 4, (
        f"node2 refused the entries of {len(refused)} append_entries requests while replaying, "
        "so the leader is not backing off"
    )

    # Only one thread of the Raft event loop may wait for the replay at a time, and it has to
    # give up on its own deadline. Before the fix no thread ever stopped waiting, so the waits
    # piled up until the pool was exhausted.
    events = waiting_thread_events(node2)
    logging.info("node2 waits for log preprocessing: %s", events)
    waiting = 0
    for timestamp, thread, delta in events:
        waiting += delta
        assert waiting <= 1, (
            f"{waiting} threads of the Raft event loop were waiting for log preprocessing at "
            f"{timestamp}, when thread {thread} started waiting"
        )
    assert waiting == 0, "a thread of the Raft event loop is still waiting for log preprocessing"

    # The deadline is 200 ms here and the replay takes seconds, so every wait must have ended on
    # the deadline rather than because the replay finished underneath it.
    assert grep_log(node2, f"{WAIT_STOPPED}, preprocessed=false"), (
        "no wait for log preprocessing ended on its deadline"
    )

    # 6) node2 is a working member of the cluster again.
    zk = get_fake_zk(node2)
    try:
        for i in range(10):
            assert zk.exists(f"/ahead_of_node2_{i}") is not None
    finally:
        zk.stop()
        zk.close()

    zk = get_fake_zk(keeper_utils.get_leader(cluster, ALL_NODES))
    try:
        zk.create("/after_replay", b"ok")
    finally:
        zk.stop()
        zk.close()


def test_divergent_local_logs_are_reconciled(started_cluster):
    """A node that restarts with a local tail the leader does not have must still be corrected.

    Only a request carrying entries makes `GotAppendEntryReqFromLeader` lower
    `last_log_idx_on_disk` to the index the two logs still match at - an empty one returns from
    that callback immediately. So the node may not ask the leader to pause until it knows its
    replay can finish without such a request, and this is the case where it cannot.
    """
    keeper_utils.wait_nodes(cluster, ALL_NODES)

    # 1) Make node2 the leader, then take its quorum away, so that whatever it appends from now
    #    on can never commit and will have to be rolled back.
    keeper_utils.send_4lw_cmd(cluster, node2, "rqld")
    for _ in range(60):
        if keeper_utils.is_leader(cluster, node2):
            break
        time.sleep(0.5)
    else:
        raise Exception("node2 did not become the leader")

    zk = get_fake_zk(node2)
    try:
        node1.stop_clickhouse(kill=True)
        node3.stop_clickhouse(kill=True)
        # This write is appended to node2's log and can never commit, so it is the entry that
        # diverges. It is expected to fail, and node2 may have dropped the session by then.
        try:
            zk.create("/diverged", b"")
        except Exception as e:
            logging.info("the write on the leader without a quorum failed, as expected: %s", e)
    finally:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass

    # 2) node2 goes away with its divergent tail on disk and no snapshot covering it.
    node2.stop_clickhouse(kill=True)

    # 3) node1 and node3 come back, elect a leader at a higher term and write on top, so their
    #    log and node2's disagree from node2's tail on. They have to be started together: each
    #    one waits for a quorum that needs the other.
    pool = Pool(2)
    try:
        pool.map(start_and_connect, [node1, node3])
    finally:
        pool.close()
        pool.join()

    zk = get_fake_zk(keeper_utils.get_leader(cluster, [node1, node3]))
    try:
        for i in range(10):
            zk.create(f"/written_without_node2_{i}", b"")
    finally:
        zk.stop()
        zk.close()

    # 4) node2 comes back and has to be told where the logs match before it can finish.
    node2.start_clickhouse(start_wait_sec=240)
    keeper_utils.wait_until_connected(cluster, node2, timeout=240)

    # Only a request carrying entries reaches this callback, and the boundary moves down only
    # when the leader's idea of where the logs match is below node2's own tail. Both together
    # are the divergence: the setup produced one, and node2 was told about it.
    corrections = [
        (int(match.group(1)), int(match.group(2)))
        for line in grep_log(node2, "GotAppendEntryReqFromLeader callback")
        for match in [TAIL_CORRECTION.search(line)]
        if match is not None
    ]
    logging.info("node2 local tail corrections (last_log_idx, last_log_idx_on_disk): %s", corrections)
    assert any(leader_idx < own_tail for leader_idx, own_tail in corrections), (
        "node2 was never told that its local tail goes beyond the leader's log, so either the "
        "setup produced no divergence or the leader was paused before it could say so"
    )

    # And NuRaft really did overwrite the entries only node2 had.
    assert grep_log(node2, "rollback logs:"), (
        "node2 never rolled back its divergent tail"
    )

    # The same bound as in the test above has to hold on this path as well.
    waiting = 0
    for timestamp, thread, delta in waiting_thread_events(node2):
        waiting += delta
        assert waiting <= 1, (
            f"{waiting} threads of the Raft event loop were waiting for log preprocessing at "
            f"{timestamp}, when thread {thread} started waiting"
        )

    # 5) node2 agrees with the rest again: it dropped what only it had and took what it missed.
    zk = get_fake_zk(node2)
    try:
        assert zk.exists("/diverged") is None
        for i in range(10):
            assert zk.exists(f"/written_without_node2_{i}") is not None
    finally:
        zk.stop()
        zk.close()

    zk = get_fake_zk(keeper_utils.get_leader(cluster, ALL_NODES))
    try:
        zk.create("/after_reconciliation", b"ok")
    finally:
        zk.stop()
        zk.close()
