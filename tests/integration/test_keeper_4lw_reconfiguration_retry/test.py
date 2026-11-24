#!/usr/bin/env python3
"""
`rcfg` makes as many attempts per action as its `retry` field asks (one retry when it is absent),
never waits past `max_total_wait_time_ms`, queues an action again once its previous copy was
accepted (never while that copy is still waiting, whatever else is queued), and rejects a negative
`retry` before running any action.
"""

import json
import re
import threading
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/enable_keeper.xml"], stay_alive=True
)

# Nothing listens on this port, so a member added with this endpoint never joins.
UNREACHABLE_ENDPOINT = "node:9235"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node)
        yield cluster
    finally:
        cluster.shutdown()


def send_rcfg(command):
    return json.loads(
        keeper_utils.send_4lw_cmd(
            cluster,
            node,
            cmd="rcfg",
            port=9181,
            argument=json.dumps(command),
            timeout_sec=120,
        )
    )


def attempts_logged(member_id):
    return int(node.count_in_log(f"(Add server {member_id}) to be applied, attempt"))


def errors_logged(member_id):
    return int(
        node.count_in_log(f"configuration update (Add server {member_id}) to happen")
    )


def attempts_logged_since(member_id, before):
    # The log is written asynchronously but in order; the command's error comes after its attempts.
    attempts_before, errors_before = before
    deadline = time.monotonic() + 30
    while errors_logged(member_id) <= errors_before and time.monotonic() < deadline:
        time.sleep(0.5)
    return attempts_logged(member_id) - attempts_before


def waits_logged(member_id):
    lines = node.grep_in_log(
        f"(Add server {member_id}) to be applied, will wait for", only_latest=True
    )
    return [int(ms) for ms in re.findall(r"will wait for (\d+) ms", lines)]


def queue_events(*member_ids):
    # These members' "pushed" and "accepted" update lines, in the order they were logged.
    lines = node.grep_in_log("Processing config update (Add server ", only_latest=True)
    events = []
    for line in lines.splitlines():
        match = re.search(r"\(Add server (\d+)\): (pushed|accepted)$", line)
        if match and int(match.group(1)) in member_ids:
            events.append((int(match.group(1)), match.group(2)))
    return events


def max_outstanding(events, member_id):
    outstanding = 0
    result = 0
    for member, event in events:
        if member == member_id:
            outstanding += 1 if event == "pushed" else -1
            result = max(result, outstanding)
    return result


def add_unreachable_member(member_id, retry, **limits):
    action = {"add_members": [{"id": member_id, "endpoint": UNREACHABLE_ENDPOINT}]}
    if retry is not None:
        action["retry"] = retry
    before = (attempts_logged(member_id), errors_logged(member_id))
    start = time.monotonic()
    result = send_rcfg({**limits, "actions": [action]})
    return result, time.monotonic() - start, before


def test_negative_retry_is_rejected_before_any_action(started_cluster):
    result = send_rcfg(
        {
            "actions": [
                {"set_priority": [{"id": 1, "priority": 7}]},
                {
                    "add_members": [{"id": 2, "endpoint": UNREACHABLE_ENDPOINT}],
                    "retry": -1,
                },
            ]
        }
    )

    zk = keeper_utils.get_fake_zk(cluster, "node")
    try:
        config = zk.get("/keeper/config")[0].decode("utf-8")
    finally:
        zk.stop()
        zk.close()

    assert result["status"] == "error", (result, config)
    assert "'retry' must be non-negative, got -1" in result["message"], (
        result,
        config,
    )
    # The set_priority action before the invalid one was not applied either.
    assert "server.1=node:9234;participant;1" in config, (result, config)


def test_retry_count_is_honored(started_cluster):
    result, elapsed, before = add_unreachable_member(
        3, retry=3, max_action_wait_time_ms=1000
    )
    attempts = attempts_logged_since(3, before)
    assert result["status"] == "error", result
    assert "with retries count 3, attempts made 4" in result["message"], (
        result,
        elapsed,
        attempts,
    )
    assert attempts == 4, (result, elapsed, attempts)
    # Every attempt waits max_action_wait_time_ms before the next one starts.
    assert elapsed >= 4, (result, elapsed, attempts)

    result, elapsed, before = add_unreachable_member(
        4, retry=0, max_action_wait_time_ms=1000
    )
    attempts = attempts_logged_since(4, before)
    assert result["status"] == "error", result
    assert "with retries count 0, attempts made 1" in result["message"], (
        result,
        elapsed,
        attempts,
    )
    assert attempts == 1, (result, elapsed, attempts)

    result, elapsed, before = add_unreachable_member(
        8, retry=3, max_action_wait_time_ms=250
    )
    attempts = attempts_logged_since(8, before)
    assert result["status"] == "error", result
    assert "with retries count 3, attempts made 4" in result["message"], (
        result,
        elapsed,
        attempts,
    )
    assert attempts == 4, (result, elapsed, attempts)
    # An attempt waits max_action_wait_time_ms, also when that is less than a second.
    assert elapsed < 3, (result, elapsed, attempts)

    result, elapsed, before = add_unreachable_member(
        9, retry=None, max_action_wait_time_ms=250
    )
    attempts = attempts_logged_since(9, before)
    assert result["status"] == "error", result
    # Without `retry`, an action is retried once.
    assert "with retries count 1, attempts made 2" in result["message"], (
        result,
        elapsed,
        attempts,
    )
    assert attempts == 2, (result, elapsed, attempts)


def test_retries_stop_at_max_total_wait_time(started_cluster):
    waits_before = len(waits_logged(5))
    result, elapsed, before = add_unreachable_member(
        5, retry=100, max_action_wait_time_ms=1000, max_total_wait_time_ms=1200
    )
    attempts = attempts_logged_since(5, before)
    waits = waits_logged(5)[waits_before:]
    info = (result, elapsed, attempts, waits)
    assert result["status"] == "error", info
    assert "with retries count 100, attempts made 2" in result["message"], info
    assert attempts == 2, info
    # The second attempt waits only for what is left of max_total_wait_time_ms.
    assert len(waits) == 2 and waits[0] == 1000 and 0 < waits[1] <= 200, info
    # Waiting a full second again, or making all 101 attempts, would take at least 2 seconds.
    assert elapsed < 1.8, info

    result, elapsed, before = add_unreachable_member(
        6, retry=100, max_action_wait_time_ms=0
    )
    attempts = attempts_logged_since(6, before)
    assert result["status"] == "error", result
    assert "with retries count 100, attempts made 1" in result["message"], (
        result,
        elapsed,
        attempts,
    )
    assert attempts == 1, (result, elapsed, attempts)


def test_retries_do_not_pile_up_in_the_update_queue(started_cluster):
    # Members added by the other tests never join; their queued copies would delay this one.
    node.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)

    events_before = len(queue_events(7))
    result, elapsed, before = add_unreachable_member(
        7, retry=10, max_action_wait_time_ms=1000
    )
    attempts = attempts_logged_since(7, before)
    events = queue_events(7)[events_before:]
    pushes = events.count((7, "pushed"))
    accepts = events.count((7, "accepted"))
    info = (result, elapsed, attempts, pushes, accepts, max_outstanding(events, 7))

    assert result["status"] == "error", info
    assert "with retries count 10, attempts made 11" in result["message"], info
    assert attempts == 11, info
    # Each copy waits in the queue for several attempts while the previous one is joining.
    assert 1 <= accepts <= attempts - 5, info
    # The action is queued again once its previous copy was taken, never once per attempt.
    assert pushes >= 2, info
    assert max_outstanding(events, 7) <= 1, info


def test_retry_does_not_wait_for_other_queued_updates(started_cluster):
    node.restart_clickhouse()
    keeper_utils.wait_until_connected(cluster, node)

    events_before = len(queue_events(10, 11, 12))
    accepts_before = queue_events(10).count((10, "accepted"))
    command = {}

    def add_member_10():
        command["out"] = add_unreachable_member(
            10, retry=1, max_action_wait_time_ms=6000, max_total_wait_time_ms=7000
        )

    thread = threading.Thread(target=add_member_10)
    thread.start()
    deadline = time.monotonic() + 30
    while queue_events(10).count((10, "accepted")) == accepts_before:
        assert time.monotonic() < deadline, queue_events(10, 11, 12)
        time.sleep(0.1)
    # While member 10 is joining, both updates are declined, so one of them is always queued.
    for member_id in (11, 12):
        action = {"add_members": [{"id": member_id, "endpoint": UNREACHABLE_ENDPOINT}]}
        send_rcfg({"max_action_wait_time_ms": 0, "actions": [{**action, "retry": 0}]})
    thread.join()
    result, elapsed, before = command["out"]
    attempts = attempts_logged_since(10, before)
    events = queue_events(10, 11, 12)[events_before:]
    info = (result, elapsed, attempts, events)

    assert result["status"] == "error", info
    assert "with retries count 1, attempts made 2" in result["message"], info
    # The first copy was accepted, so the retry queues the action again behind the other updates.
    assert events.count((10, "pushed")) == 2, info
    second_push = [i for i, event in enumerate(events) if event == (10, "pushed")][1]
    assert (12, "pushed") in events[:second_push], info
    assert (12, "accepted") not in events[:second_push], info
    assert max_outstanding(events, 10) <= 1, info
