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

The third takes the back-off away entirely, so that the leader keeps re-sending for the whole
replay and threads reach the wait continuously. That is the only way to exercise the bound
itself: when the back-off works, a second thread never gets there.
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
ADMISSION_DECLINED = "ProcessReq callback: another thread is already waiting"
ENTRIES_REFUSED = "Logs not preprocessed, ProcessReq callback with"
NO_REPLAY_NEEDED = "No log preprocessing needed"

NODE2_CONFIG = "/etc/clickhouse-server/config.d/enable_keeper2.xml"
WAIT_FAILPOINT = "keeper_local_logs_preprocessing_wait"
NEVER_PAUSE_FAILPOINT = "keeper_never_pause_appending_entries"
# Enabled from the config rather than over SQL, because the replay - and with it the first wait -
# starts as the server comes up, before a query could reach it.
NEVER_PAUSE_ONLY = (
    "<fail_points_active>"
    f"<{NEVER_PAUSE_FAILPOINT}>1</{NEVER_PAUSE_FAILPOINT}>"
    "</fail_points_active>"
)
FAILPOINTS_ACTIVE = (
    "<fail_points_active>"
    f"<{WAIT_FAILPOINT}>1</{WAIT_FAILPOINT}>"
    f"<{NEVER_PAUSE_FAILPOINT}>1</{NEVER_PAUSE_FAILPOINT}>"
    "</fail_points_active>"
)
CONFIG_END = "</clickhouse>"

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


def count_in_log(node, pattern):
    """Number of lines matching `pattern` since the node was last started.

    Counted in the container rather than returned, because the run that disables the pause
    produces tens of thousands of these.
    """
    output = node.exec_in_container(
        ["bash", "-c", f"grep -E -c -- '{pattern}' {SERVER_LOG} || true"]
    )
    return int(output.strip() or 0)


def waiting_thread_events(node):
    """(+1 started / -1 stopped) events, in log order, for the preprocessing wait."""
    events = []
    for line in grep_log(node, f"{WAIT_STARTED}|{WAIT_STOPPED}"):
        match = LOG_LINE.match(line)
        assert match is not None, (
            f"a wait line the bound is computed from could not be read: {line!r}"
        )
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

    # Whether a thread reaches the wait at all is not something this test can force: the arm
    # that waits and the condition that pauses the leader are the same predicate, so the
    # requests that would reach the wait are the ones the pause stops. The wait, its bound and
    # its deadline are exercised by test_one_thread_waits_when_the_leader_is_never_paused, which
    # removes the pause. What belongs here is the pause itself: no thread should ever find
    # another one already waiting, because the leader is stopped before a second request
    # carrying entries arrives.
    assert not grep_log(node2, ADMISSION_DECLINED), (
        "a second thread of the Raft event loop reached the wait, so the leader was never "
        "paused - the negative batch size hint regressed"
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

    # The same bound as in the test above has to hold on this path as well, and every wait must
    # have been released: a wait that never ends is the wedge, whichever branch reached it.
    waiting = 0
    for timestamp, thread, delta in waiting_thread_events(node2):
        waiting += delta
        assert 0 <= waiting <= 1, (
            f"{waiting} threads of the Raft event loop were waiting for log preprocessing at "
            f"{timestamp}, when thread {thread} started waiting"
        )
    assert waiting == 0, "a thread of the Raft event loop is still waiting for log preprocessing"
    assert not grep_log(node2, ADMISSION_DECLINED), (
        "a second thread of the Raft event loop reached the wait, so the leader was never "
        "paused - the negative batch size hint regressed"
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


def test_one_thread_waits_when_the_leader_is_never_paused(started_cluster):
    """The bound of one waiting thread has to hold when the leader is not backing off.

    In the ordinary case the negative batch size hint stops the leader before a second request
    carrying entries can arrive, so no thread ever finds another one already waiting - which is
    what the first test asserts, and which leaves the refusal itself unexercised.
    `keeper_never_pause_appending_entries` takes the hint away, so the leader re-sends every
    batch as fast as it is refused and threads of the Raft event loop reach the wait
    continuously. That is the shape a regression of the hint would produce, measured at ~11000
    requests per second while this fix was being written. One thread may wait; every other has
    to be turned away and its request handled like any other that arrives before the replay is
    over, which is what keeps the pool from draining.
    """
    keeper_utils.wait_nodes(cluster, ALL_NODES)

    # 1) A tail for node2 to replay, as in the first test.
    zk = get_fake_zk(keeper_utils.get_leader(cluster, ALL_NODES))
    try:
        zk.create("/unpaused")
        for transaction in range(TRANSACTIONS):
            request = zk.transaction()
            for i in range(CREATES_PER_TRANSACTION):
                request.create(f"/unpaused/n{transaction:05d}_{i:05d}", b"")
            request.commit()
    finally:
        zk.stop()
        zk.close()

    last_znode = f"/unpaused/n{TRANSACTIONS - 1:05d}_{CREATES_PER_TRANSACTION - 1:05d}"
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

    node2.stop_clickhouse(kill=True)

    try:
        # 2) Take the hint away from node2 only, so the leader keeps entries coming and threads
        #    reach the wait one after another. Nothing holds them here, so each one runs its own
        #    course - which is the only way to see the wait end on its deadline.
        node2.replace_in_config(NODE2_CONFIG, CONFIG_END, NEVER_PAUSE_ONLY + CONFIG_END)

        zk = get_fake_zk(keeper_utils.get_leader(cluster, [node1, node3]))
        try:
            for i in range(10):
                zk.create(f"/ahead_of_unpaused_{i}", b"")
        finally:
            zk.stop()
            zk.close()

        node2.start_clickhouse(start_wait_sec=240)
        keeper_utils.wait_until_connected(cluster, node2, timeout=240)

        assert not grep_log(node2, NO_REPLAY_NEEDED), (
            "node2 restarted with nothing to replay, so this test checks nothing"
        )

        # The wait is bounded, and this is the assertion that says so: it came back with the logs
        # still not preprocessed, which only a deadline can produce. An unbounded wait also ends
        # eventually - the commit thread notifies it - so a wait that merely ended proves nothing.
        for _ in range(240):
            if count_in_log(node2, f"{WAIT_STOPPED}, preprocessed=false"):
                break
            time.sleep(0.5)
        else:
            raise Exception("no wait for log preprocessing ended on its deadline")

        # 3) Now the bound on how many threads may wait at once, which needs one of them held
        #    while another arrives. The deadline is shorter than the interval after which the
        #    leader re-sends, by construction, so that overlap cannot be produced by timing -
        #    hence the failpoint, and hence a second restart: armed from the start it parks the
        #    first wait, and then no wait would have run its course above.
        zk = get_fake_zk(keeper_utils.get_leader(cluster, ALL_NODES))
        try:
            zk.create("/unpaused_parked")
            for transaction in range(TRANSACTIONS):
                request = zk.transaction()
                for i in range(CREATES_PER_TRANSACTION):
                    request.create(f"/unpaused_parked/n{transaction:05d}_{i:05d}", b"")
                request.commit()
        finally:
            zk.stop()
            zk.close()

        node2.stop_clickhouse(kill=True)
        node2.replace_in_config(NODE2_CONFIG, NEVER_PAUSE_ONLY, FAILPOINTS_ACTIVE)
        node2.start_clickhouse(start_wait_sec=240)
        keeper_utils.wait_until_connected(cluster, node2, timeout=240)

        assert not grep_log(node2, NO_REPLAY_NEEDED), (
            "node2 restarted with nothing to replay, so this test checks nothing"
        )

        # One thread is parked at the failpoint; the leader re-sends to a peer that stopped
        # answering, and that request is what meets the occupied gate.
        for _ in range(240):
            if count_in_log(node2, ADMISSION_DECLINED):
                break
            time.sleep(0.5)
        else:
            raise Exception(
                "no thread reached the admission gate while one was parked at the failpoint"
            )

        # Release it, so the replay can finish.
        node2.query(f"SYSTEM DISABLE FAILPOINT {WAIT_FAILPOINT}")

        # The refusal has to have happened, or the leader backed off for some other reason and
        # this is the first test with extra steps.
        declined = count_in_log(node2, ADMISSION_DECLINED)
        refused = count_in_log(node2, ENTRIES_REFUSED)
        logging.info(
            "node2 refused the entries of %s append_entries requests and turned away %s "
            "threads at the wait",
            refused,
            declined,
        )
        assert declined, (
            "no thread was turned away at the wait, so the leader stopped sending entries even "
            "with the pause disabled and the bound was never put under pressure"
        )

        # And the bound held under that pressure, which is the whole point of this test.
        events = waiting_thread_events(node2)
        assert events, (
            "no thread entered the wait, so the bound below is checked against nothing - the "
            "failpoint held one, so its start and its release both have to be in the log"
        )
        waiting = 0
        for timestamp, thread, delta in events:
            waiting += delta
            assert 0 <= waiting <= 1, (
                f"{waiting} threads of the Raft event loop were waiting for log preprocessing "
                f"at {timestamp}, when thread {thread} started waiting"
            )
        assert waiting == 0, (
            "a thread of the Raft event loop is still waiting for log preprocessing"
        )

        # A turned-away thread must not cost the replay its progress: its request is handled to
        # the end rather than declined, so the commit index it carries still lands. Catching up
        # does not isolate that contribution - a later request carries the same commit index or a
        # newer one - so this asserts that the node recovers, not which request got it there.
        zk = get_fake_zk(node2)
        try:
            for i in range(10):
                assert zk.exists(f"/ahead_of_unpaused_{i}") is not None
        finally:
            zk.stop()
            zk.close()
    finally:
        # Restoring the config only takes effect on the next start, so both fail points have to
        # be turned off in the process that is running. A paused one is released by nothing but
        # disabling it - not by shutdown - so a failure before the release above would otherwise
        # leave an asio worker blocked and the Raft instance unable to join its pool. The other
        # is not paused but stays enabled until told otherwise, which any later test would
        # inherit. Disabling one that is not enabled is a no-op.
        for fail_point in (WAIT_FAILPOINT, NEVER_PAUSE_FAILPOINT):
            try:
                node2.query(f"SYSTEM DISABLE FAILPOINT {fail_point}")
            except Exception as e:  # the server may be gone; this must not mask the real failure
                logging.info("could not disable %s: %s", fail_point, e)
        node2.replace_in_config(NODE2_CONFIG, FAILPOINTS_ACTIVE + CONFIG_END, CONFIG_END)
        node2.replace_in_config(NODE2_CONFIG, NEVER_PAUSE_ONLY + CONFIG_END, CONFIG_END)
