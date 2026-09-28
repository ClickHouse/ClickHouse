#!/usr/bin/env python3
"""
`rcfg` makes as many attempts per action as its `retry` field asks, stops retrying once
`max_total_wait_time_ms` is used up, does not queue an action again while its previous copy is still
waiting to be applied, and rejects a negative `retry` before running any action.
"""

import json
import re
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


def attempts_logged_since(member_id, before, reported):
    # The server log is written asynchronously, so the last lines can appear after rcfg returns.
    deadline = time.monotonic() + 30
    while (
        attempts_logged(member_id) - before < reported and time.monotonic() < deadline
    ):
        time.sleep(0.5)
    time.sleep(1)
    return attempts_logged(member_id) - before


def queue_events(member_id):
    # This member's "pushed" and "accepted" update lines, in the order they were logged.
    lines = node.grep_in_log(
        f"Processing config update (Add server {member_id}): ", only_latest=True
    )
    return [
        line.rsplit(": ", 1)[1]
        for line in lines.splitlines()
        if line.endswith((": pushed", ": accepted"))
    ]


def add_unreachable_member(member_id, retry, **limits):
    before = attempts_logged(member_id)
    start = time.monotonic()
    result = send_rcfg(
        {
            **limits,
            "actions": [
                {
                    "add_members": [
                        {"id": member_id, "endpoint": UNREACHABLE_ENDPOINT}
                    ],
                    "retry": retry,
                }
            ],
        }
    )
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
    attempts = attempts_logged_since(3, before, reported=4)
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
    attempts = attempts_logged_since(4, before, reported=1)
    assert result["status"] == "error", result
    assert "with retries count 0, attempts made 1" in result["message"], (
        result,
        elapsed,
        attempts,
    )
    assert attempts == 1, (result, elapsed, attempts)


def test_retries_stop_at_max_total_wait_time(started_cluster):
    result, elapsed, before = add_unreachable_member(
        5, retry=100, max_action_wait_time_ms=1000, max_total_wait_time_ms=2500
    )
    assert result["status"] == "error", result
    m = re.search(r"attempts made (\d+)", result["message"])
    assert m, (result, elapsed)
    made = int(m.group(1))
    assert 2 <= made <= 4, (result, elapsed)
    attempts = attempts_logged_since(5, before, reported=made)
    assert attempts == made, (result, elapsed, attempts)
    # Making all 101 attempts would take about 101 seconds.
    assert elapsed < 30, (result, elapsed, attempts)

    result, elapsed, before = add_unreachable_member(
        6, retry=100, max_action_wait_time_ms=0
    )
    attempts = attempts_logged_since(6, before, reported=1)
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
    attempts = attempts_logged_since(7, before, reported=11)
    events = queue_events(7)[events_before:]
    pushes = events.count("pushed")
    accepts = events.count("accepted")
    outstanding = 0
    max_outstanding = 0
    for event in events:
        outstanding += 1 if event == "pushed" else -1
        max_outstanding = max(max_outstanding, outstanding)
    info = (result, elapsed, attempts, pushes, accepts, max_outstanding)

    assert result["status"] == "error", info
    assert "with retries count 10, attempts made 11" in result["message"], info
    assert attempts == 11, info
    # Each copy waits in the queue for several attempts while the previous one is joining.
    assert 1 <= accepts <= attempts - 5, info
    # The action is queued again once its previous copy was taken, never once per attempt.
    assert pushes >= 2, info
    assert max_outstanding <= 2, info
