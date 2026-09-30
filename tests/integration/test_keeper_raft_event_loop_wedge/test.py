#!/usr/bin/env python3
"""
A Keeper node that restarts with local logs to replay must keep its Raft event loop running.

The replay used to block event loop threads without a deadline, one per leader reconnect, until the
node dropped out of Raft. The tests check that the leader backs off, that at most one thread waits,
and that a divergent tail or an installed snapshot does not stop the replay from finishing.
"""

import logging
import re
import threading
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

# One transaction is one log entry, so a short changelog takes seconds to replay.
TRANSACTIONS = 150
CREATES_PER_TRANSACTION = 1000

WAIT_STARTED = "ProcessReq callback: waiting for preprocessing"
WAIT_STOPPED = "ProcessReq callback: stopped waiting for preprocessing"
ADMISSION_DECLINED = "ProcessReq callback: another thread is already waiting"
ENTRIES_REFUSED = "Logs not preprocessed, ProcessReq callback with"
NO_REPLAY_NEEDED = "No log preprocessing needed"
# The request that finds everything on disk already committed ends the replay itself.
PREPROCESSED_ON_REQUEST = "ProcessReq callback: preprocessing logs"

NODE2_CONFIG = "/etc/clickhouse-server/config.d/enable_keeper2.xml"
WAIT_FAILPOINT = "keeper_local_logs_preprocessing_wait"
NEVER_PAUSE_FAILPOINT = "keeper_never_pause_appending_entries"
# Set in the config: the first wait happens during startup, before SQL is available.
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


def waiting_thread_events(node):
    """(+1 started / -1 stopped) events, in log order, for the preprocessing wait."""
    events = []
    # Every start rotates the log, so the current file covers exactly this run.
    lines = node.grep_in_log(
        f"{WAIT_STARTED}\\|{WAIT_STOPPED}", only_latest=True
    ).splitlines()
    for line in lines:
        match = LOG_LINE.match(line)
        assert match is not None, (
            f"a wait line the bound is computed from could not be read: {line!r}"
        )
        events.append((match.group(1), match.group(2), -1 if WAIT_STOPPED in line else +1))
    return events


def test_raft_event_loop_is_not_wedged_by_log_replay(started_cluster):
    keeper_utils.wait_nodes(cluster, ALL_NODES)

    # 1) Build the tail node2 will replay (no snapshots are taken).
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

    # 2) Wait until node2's own changelog has the whole tail.
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

    # 3) SIGKILL leaves no shutdown snapshot, so node2 restarts with the whole tail to replay.
    node2.stop_clickhouse(kill=True)

    # 4) Put the leader ahead, so it has entries to send after the restart.
    zk = get_fake_zk(node1)
    try:
        for i in range(10):
            zk.create(f"/ahead_of_node2_{i}", b"")
    finally:
        zk.stop()
        zk.close()

    # 5) Restart node2; it replays.
    node2.start_clickhouse(start_wait_sec=240)
    keeper_utils.wait_until_connected(cluster, node2, timeout=240)

    # The bug needs a replay.
    assert not node2.grep_in_log(
        NO_REPLAY_NEEDED, only_latest=True
    ).splitlines(), (
        "node2 restarted with nothing to replay, so this test checks nothing"
    )

    # The leader reached node2 during the replay, and backed off after a few refusals.
    refused = node2.grep_in_log(ENTRIES_REFUSED, only_latest=True).splitlines()
    logging.info("node2 refused the entries of %s append_entries requests", len(refused))
    assert refused, "the leader never sent entries to node2 while it was replaying"
    assert len(refused) <= 4, (
        f"node2 refused the entries of {len(refused)} append_entries requests while replaying, "
        "so the leader is not backing off"
    )

    # The bound on waiters and the deadline are covered by
    # test_one_thread_waits_when_the_leader_is_never_paused, where a failpoint makes them certain.

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


def check_divergent_local_logs_are_reconciled(snapshot_at_common_index):
    """A node restarting with a tail the leader does not have is rolled back and recovers."""
    keeper_utils.wait_nodes(cluster, ALL_NODES)
    # Both callers run on the same cluster, so each writes its own znodes.
    root = f"/divergent_{int(snapshot_at_common_index)}"

    # 1) Make node2 the leader, then take its quorum away, so its next entry diverges.
    keeper_utils.send_4lw_cmd(cluster, node2, "rqld")
    for _ in range(60):
        if keeper_utils.is_leader(cluster, node2):
            break
        time.sleep(0.5)
    else:
        raise Exception("node2 did not become the leader")
    # A leader that has just taken over may still refuse new sessions.
    keeper_utils.wait_until_connected(cluster, node2)

    snapshot_idx = None
    zk = get_fake_zk(node2)
    try:
        node1.stop_clickhouse(kill=True)
        node3.stop_clickhouse(kill=True)
        if snapshot_at_common_index:
            # Nothing can commit any more, so this is the last index the logs will share.
            snapshot_idx = keeper_utils.send_4lw_cmd(cluster, node2, cmd="csnp").strip()
            assert (
                snapshot_idx.isdigit()
            ), f"csnp did not return a log index: {snapshot_idx!r}"
            node2.wait_for_log_line(
                f"Created persistent snapshot {snapshot_idx} with path"
            )
        # The diverging entry; the write is expected to fail.
        try:
            zk.create(f"{root}_diverged", b"")
        except Exception as e:
            logging.info("the write on the leader without a quorum failed, as expected: %s", e)
    finally:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass

    # 2) node2 stops with the divergent tail on disk.
    node2.stop_clickhouse(kill=True)

    # 3) node1 and node3 restart together (each needs the other for a quorum) and write more.
    pool = Pool(2)
    try:
        pool.map(start_and_connect, [node1, node3])
    finally:
        pool.close()
        pool.join()

    zk = get_fake_zk(keeper_utils.get_leader(cluster, [node1, node3]))
    try:
        for i in range(10):
            zk.create(f"{root}_written_without_node2_{i}", b"")
    finally:
        zk.stop()
        zk.close()

    # 4) node2 restarts and has to learn where the logs match.
    node2.start_clickhouse(start_wait_sec=240)
    keeper_utils.wait_until_connected(cluster, node2, timeout=240)

    # node2 was told that its tail goes beyond the leader's log.
    corrections = [
        (int(match.group(1)), int(match.group(2)))
        for line in node2.grep_in_log(
            "GotAppendEntryReqFromLeader callback", only_latest=True
        ).splitlines()
        for match in [TAIL_CORRECTION.search(line)]
        if match is not None
    ]
    logging.info("node2 local tail corrections (last_log_idx, last_log_idx_on_disk): %s", corrections)
    assert any(leader_idx < own_tail for leader_idx, own_tail in corrections), (
        "node2 was never told that its local tail goes beyond the leader's log, so either the "
        "setup produced no divergence or the leader was paused before it could say so"
    )

    if snapshot_at_common_index:
        # Rolled back onto the snapshot, and a request carrying entries ended the replay.
        assert any(
            leader_idx == int(snapshot_idx) < own_tail
            for leader_idx, own_tail in corrections
        ), f"node2 was not rolled back onto its snapshot {snapshot_idx}: {corrections}"
        assert node2.grep_in_log(
            PREPROCESSED_ON_REQUEST, only_latest=True
        ).splitlines(), "the replay of node2 was not ended by a request carrying entries"

    # And NuRaft really did overwrite the entries only node2 had.
    assert node2.grep_in_log(
        "rollback logs:", only_latest=True
    ).splitlines(), "node2 never rolled back its divergent tail"

    # At most one waiter, and every wait was released.
    waiting = 0
    for timestamp, thread, delta in waiting_thread_events(node2):
        waiting += delta
        assert 0 <= waiting <= 1, (
            f"{waiting} threads of the Raft event loop were waiting for log preprocessing at "
            f"{timestamp}, when thread {thread} started waiting"
        )
    assert waiting == 0, "a thread of the Raft event loop is still waiting for log preprocessing"

    # 5) node2 dropped its divergent entry and caught up.
    zk = get_fake_zk(node2)
    try:
        assert zk.exists(f"{root}_diverged") is None
        for i in range(10):
            assert zk.exists(f"{root}_written_without_node2_{i}") is not None
    finally:
        zk.stop()
        zk.close()

    zk = get_fake_zk(keeper_utils.get_leader(cluster, ALL_NODES))
    try:
        zk.create(f"{root}_after_reconciliation", b"ok")
    finally:
        zk.stop()
        zk.close()


def test_divergent_local_logs_are_reconciled(started_cluster):
    check_divergent_local_logs_are_reconciled(snapshot_at_common_index=False)


def test_one_thread_waits_when_the_leader_is_never_paused(started_cluster):
    """At most one thread waits even when the leader keeps re-sending entries (pause disabled)."""
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
        # 2) Disable the pause on node2 only; waits now run to their deadline.
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

        assert not node2.grep_in_log(
            NO_REPLAY_NEEDED, only_latest=True
        ).splitlines(), (
            "node2 restarted with nothing to replay, so this test checks nothing"
        )

        # Returning with the logs still not preprocessed means the wait hit its deadline.
        for _ in range(240):
            if int(node2.count_in_log(f"{WAIT_STOPPED}, preprocessed=false")):
                break
            time.sleep(0.5)
        else:
            raise Exception("no wait for log preprocessing ended on its deadline")

        # 3) Park the first waiter at a failpoint (armed from startup, hence a second restart), so
        #    that later requests meet the admission gate.
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

        # node2 already has the whole tail, so keep writing from before the restart: the leader
        # then has entries to send while one thread is parked.
        stop_writing = threading.Event()

        def keep_writing():
            writer_zk = get_fake_zk(keeper_utils.get_leader(cluster, [node1, node3]))
            try:
                seq = 0
                while not stop_writing.is_set():
                    writer_zk.create(f"/unpaused_stream_{seq:06d}", b"")
                    seq += 1
            except Exception as e:
                logging.info("the writer feeding the parked gate stopped: %s", e)
            finally:
                try:
                    writer_zk.stop()
                    writer_zk.close()
                except Exception:
                    pass

        writer = threading.Thread(target=keep_writing)
        writer.start()
        try:
            node2.start_clickhouse(start_wait_sec=240)
            keeper_utils.wait_until_connected(cluster, node2, timeout=240)

            assert not node2.grep_in_log(
                NO_REPLAY_NEEDED, only_latest=True
            ).splitlines(), (
                "node2 restarted with nothing to replay, so this test checks nothing"
            )

            for _ in range(240):
                if int(node2.count_in_log(ADMISSION_DECLINED)):
                    break
                time.sleep(0.5)
            else:
                raise Exception(
                    "no thread reached the admission gate while one was parked at the failpoint"
                )
        finally:
            stop_writing.set()
            writer.join(timeout=60)

        # Release it, so the replay can finish.
        node2.query(f"SYSTEM DISABLE FAILPOINT {WAIT_FAILPOINT}")

        # Some thread was turned away at the gate.
        declined = int(node2.count_in_log(ADMISSION_DECLINED))
        refused = int(node2.count_in_log(ENTRIES_REFUSED))
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

        # And the bound held.
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

        # node2 recovers.
        zk = get_fake_zk(node2)
        try:
            for i in range(10):
                assert zk.exists(f"/ahead_of_unpaused_{i}") is not None
        finally:
            zk.stop()
            zk.close()
    finally:
        # The config change applies only on restart, and a paused failpoint blocks an asio worker
        # until disabled, so turn both off in the running process.
        for fail_point in (WAIT_FAILPOINT, NEVER_PAUSE_FAILPOINT):
            try:
                node2.query(f"SYSTEM DISABLE FAILPOINT {fail_point}")
            except Exception as e:  # the server may be gone; this must not mask the real failure
                logging.info("could not disable %s: %s", fail_point, e)
        node2.replace_in_config(NODE2_CONFIG, FAILPOINTS_ACTIVE + CONFIG_END, CONFIG_END)
        node2.replace_in_config(NODE2_CONFIG, NEVER_PAUSE_ONLY + CONFIG_END, CONFIG_END)


# After the tests that need long replays: its snapshot on node2 shortens later ones.
def test_divergent_local_logs_over_a_snapshot_are_reconciled(started_cluster):
    """Rolled back onto a snapshot: nothing is left to commit, so a request with entries ends it."""
    check_divergent_local_logs_are_reconciled(snapshot_at_common_index=True)


def get_log_info(node):
    data = keeper_utils.send_4lw_cmd(cluster, node, cmd="lgif")
    return dict(line.split("\t") for line in data.splitlines() if "\t" in line)


# Last: it compacts node1 and node3, so a node behind them can only catch up by a snapshot.
def test_replay_overtaken_by_an_installed_snapshot_is_finished(started_cluster):
    """A snapshot installed during the replay: nothing is left to commit, so a request carrying
    entries ends it."""
    keeper_utils.wait_nodes(cluster, ALL_NODES)
    root = "/installed_snapshot"

    # 1) Entries node2 will replay.
    zk = get_fake_zk(keeper_utils.get_leader(cluster, ALL_NODES))
    try:
        zk.create(root)
        for i in range(10):
            zk.create(f"{root}/before_{i}", b"")
    finally:
        zk.stop()
        zk.close()

    zk = get_fake_zk(node2)
    try:
        for _ in range(120):
            if zk.exists(f"{root}/before_9") is not None:
                break
            time.sleep(0.5)
        else:
            raise Exception("node2 did not receive the entries it is going to replay")
    finally:
        zk.stop()
        zk.close()

    node2.stop_clickhouse(kill=True)

    # 2) Write past node2's tail, then snapshot and compact both possible leaders. The session is
    #    closed first, so nothing is left for the leader to send right after the install.
    zk = get_fake_zk(keeper_utils.get_leader(cluster, [node1, node3]))
    try:
        for i in range(10):
            zk.create(f"{root}/without_node2_{i}", b"")
    finally:
        zk.stop()
        zk.close()

    for node in (node1, node3):
        snapshot_idx = keeper_utils.send_4lw_cmd(cluster, node, cmd="csnp").strip()
        assert (
            snapshot_idx.isdigit()
        ), f"csnp did not return a log index: {snapshot_idx!r}"
        node.wait_for_log_line(f"Created persistent snapshot {snapshot_idx} with path")
        for _ in range(60):
            if int(get_log_info(node)["first_log_idx"]) >= int(snapshot_idx):
                break
            time.sleep(0.5)
        else:
            raise Exception(f"{node.name} did not compact its log behind the snapshot")

    # 3) node2 replays, is sent the snapshot and installs it.
    node2.start_clickhouse(start_wait_sec=240)
    node2.wait_for_log_line("KeeperStateMachine: Applying snapshot", timeout=120)

    assert not node2.grep_in_log(
        NO_REPLAY_NEEDED, only_latest=True
    ).splitlines(), "node2 restarted with nothing to replay, so this test checks nothing"
    last_local = int(
        re.search(
            r"Last local log idx (\d+)",
            node2.grep_in_log("Last local log idx", only_latest=True),
        ).group(1)
    )
    installed = int(
        re.search(
            r"Applying snapshot (\d+)",
            node2.grep_in_log("Applying snapshot", only_latest=True),
        ).group(1)
    )
    assert installed >= last_local, (
        f"the snapshot {installed} does not cover node2's tail {last_local}, so a commit would "
        "still end the replay and this test checks nothing"
    )

    # Wait for the install to finish, not just to start.
    for _ in range(240):
        if int(get_log_info(node2)["last_committed_log_idx"]) >= installed:
            break
        time.sleep(0.5)
    else:
        raise Exception(f"node2 did not finish installing the snapshot {installed}")

    # Let a few post-install heartbeats (100 ms) go by before writing.
    time.sleep(1)

    zk = get_fake_zk(keeper_utils.get_leader(cluster, [node1, node3]))
    try:
        zk.create(f"{root}/after_install", b"")
    finally:
        zk.stop()
        zk.close()

    keeper_utils.wait_until_connected(cluster, node2, timeout=240)
    assert node2.grep_in_log(
        PREPROCESSED_ON_REQUEST, only_latest=True
    ).splitlines(), "the replay of node2 was not ended by a request carrying entries"

    zk = get_fake_zk(node2)
    try:
        for i in range(10):
            assert zk.exists(f"{root}/before_{i}") is not None
            assert zk.exists(f"{root}/without_node2_{i}") is not None
        assert zk.exists(f"{root}/after_install") is not None
    finally:
        zk.stop()
        zk.close()
