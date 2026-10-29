#!/usr/bin/env python3
"""
Regression test for NuRaft null pointer dereference when a node becomes leader
after a rolling membership change that leaves abandoned peer objects.

The crash sequence (without the fix):
  1. Start a 3-node cluster (node1, node2, node3) with node1 as leader.
  2. Add node4 to the cluster. node4 creates peer objects for nodes 1, 2, 3.
  3. Remove node2. node2 receives `leave_cluster_req`, setting
     `steps_to_down_ = 2`. After the leader stops heartbeating node2, the
     election timer fires twice, decrementing `steps_to_down_` to 0, which
     triggers `cancel_schedulers()`. `cancel_schedulers()` calls
     `peer::shutdown()` on all of node2's peer objects, setting
     `hb_task_ = null` and `is_shutdown_ = true`.
  4. Add node5 to the cluster.
  5. Remove node3. Same `cancel_schedulers()` / `peer::shutdown()` sequence
     fires on node3's peer objects.
  6. Add node6 to the cluster.
  7. Transfer leadership to node4. `become_leader()` calls
     `enable_hb_for_peer()` for each peer in node4's `peers_` list.
     Without the fix, if any peer has `hb_task_ = null` after `shutdown()`,
     `schedule_task()` dereferences a null pointer and crashes.

The fix (NuRaft PR #91) adds an `is_shutdown_` flag to `peer` and a check in
`enable_hb_for_peer()`: if `p.is_shutdown()`, call `p.reopen()` to recreate
`hb_task_` before scheduling the heartbeat timer.
"""

import json
import os
import re
import time
import uuid
from typing import NamedTuple

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")


def _from_template(template_name: str, suffix: str, node_names: dict) -> str:
    """Read a config template, substitute {nodeN} placeholders with the actual
    hostnames for this run, write the result next to the template, and return
    the output filename."""
    output_name = template_name.replace(".xml", f"_{suffix}.xml")
    with open(os.path.join(CONFIG_DIR, template_name)) as f:
        content = f.read().format(**{f"node{i}": name for i, name in node_names.items()})
    with open(os.path.join(CONFIG_DIR, output_name), "w") as f:
        f.write(content)
    return output_name


class ClusterNodes(NamedTuple):
    cluster: ClickHouseCluster
    node_names: dict  # {1: "node1_<suffix>", ...}
    node1: object
    node2: object
    node3: object
    node4: object
    node5: object
    node6: object


@pytest.fixture(scope="function")
def started_cluster():
    suffix = uuid.uuid4().hex[:8]
    node_names = {i: f"node{i}_{suffix}" for i in range(1, 7)}

    cfg1 = _from_template("enable_keeper1.xml", suffix, node_names)
    cfg2 = _from_template("enable_keeper2.xml", suffix, node_names)
    cfg3 = _from_template("enable_keeper3.xml", suffix, node_names)
    cfg4 = _from_template("enable_keeper4.xml", suffix, node_names)
    cfg5 = _from_template("enable_keeper5.xml", suffix, node_names)
    cfg6 = _from_template("enable_keeper6.xml", suffix, node_names)

    cluster = ClickHouseCluster(__file__)
    node1 = cluster.add_instance(
        node_names[1],
        main_configs=[f"configs/{cfg1}"],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    node2 = cluster.add_instance(
        node_names[2],
        main_configs=[f"configs/{cfg2}"],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    node3 = cluster.add_instance(
        node_names[3],
        main_configs=[f"configs/{cfg3}"],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    node4 = cluster.add_instance(
        node_names[4],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    node5 = cluster.add_instance(
        node_names[5],
        stay_alive=True,
        with_remote_database_disk=False,
    )
    node6 = cluster.add_instance(
        node_names[6],
        stay_alive=True,
        with_remote_database_disk=False,
    )

    nodes = ClusterNodes(
        cluster=cluster,
        node_names=node_names,
        node1=node1,
        node2=node2,
        node3=node3,
        node4=node4,
        node5=node5,
        node6=node6,
    )
    try:
        cluster.start()
        yield nodes
    finally:
        cluster.shutdown()
        for cfg in [cfg1, cfg2, cfg3, cfg4, cfg5, cfg6]:
            try:
                os.remove(os.path.join(CONFIG_DIR, cfg))
            except OSError:
                pass


def _relax_timeouts_for_sanitizer(config_path):
    """Rewrite a keeper config with relaxed Raft timeouts for sanitizer builds.

    Under sanitizers (ASan, MSan, TSan) CPU overhead can delay Raft
    heartbeats enough to trigger spurious leader elections. This bumps
    the heartbeat interval and election timeout so the cluster stays
    stable even with 3-5x slowdown.
    """
    with open(config_path) as f:
        content = f.read()

    content = re.sub(
        r"<heart_beat_interval_ms>\d+</heart_beat_interval_ms>",
        "<heart_beat_interval_ms>2000</heart_beat_interval_ms>",
        content,
    )
    content = re.sub(
        r"<election_timeout_lower_bound_ms>\d+</election_timeout_lower_bound_ms>",
        "<election_timeout_lower_bound_ms>5000</election_timeout_lower_bound_ms>",
        content,
    )
    content = re.sub(
        r"<election_timeout_upper_bound_ms>\d+</election_timeout_upper_bound_ms>",
        "<election_timeout_upper_bound_ms>10000</election_timeout_upper_bound_ms>",
        content,
    )
    # Configs for nodes 1/2/3 omit election timeouts — add them.
    if "<election_timeout_lower_bound_ms>" not in content:
        content = re.sub(
            r"([ \t]*)</coordination_settings>",
            r"\g<1>    <election_timeout_lower_bound_ms>5000</election_timeout_lower_bound_ms>\n"
            r"\g<1>    <election_timeout_upper_bound_ms>10000</election_timeout_upper_bound_ms>\n"
            r"\g<1></coordination_settings>",
            content,
        )

    with open(config_path, "w") as f:
        f.write(content)


def send_rcfg(cluster, node, command, timeout_sec=60):
    result_str = keeper_utils.send_4lw_cmd(
        cluster,
        node,
        cmd="rcfg",
        argument=json.dumps(command),
        timeout_sec=timeout_sec,
    )
    return json.loads(result_str)


def start_keeper(node, config_name):
    """Copy a keeper config into the container and (re)start the keeper server."""
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, config_name),
        "/etc/clickhouse-server/config.d/" + config_name,
    )
    node.start_clickhouse()


def node_id(node):
    """Extract the integer server ID from a node instance (e.g. node3_ab12cd34 → 3)."""
    return int(node.name.split("_")[0].replace("node", ""))


def test_leader_election_after_rolling_membership_change(started_cluster):
    """
    Regression test: after a rolling membership change (add/remove cycles on
    followers then on the leader), the node that wins the subsequent election
    must not dereference a null pointer inside `enable_hb_for_peer`.

    Reproduces the crash from https://github.com/ClickHouse/NuRaft/pull/91.

    Strategy: replace the two followers first, then replace the leader.
    Transferring leadership to node4 or node5 triggers become_leader(), which
    carries stale peer state from the original cluster and would crash without
    the fix.
    """
    cluster = started_cluster.cluster
    node_names = started_cluster.node_names
    node1 = started_cluster.node1
    node2 = started_cluster.node2
    node3 = started_cluster.node3
    node4 = started_cluster.node4
    node5 = started_cluster.node5
    node6 = started_cluster.node6

    for n in [node1, node2, node3]:
        keeper_utils.wait_until_connected(cluster, n, timeout=60.0)

    # Under sanitizers, Raft heartbeats can be delayed enough to trigger
    # spurious leader elections.  Detect and relax timeouts if needed.
    if node1.is_built_with_sanitizer():
        # Patch all 6 config files on disk.  Nodes 4/5/6 will pick them
        # up later when start_keeper copies them into the container.
        for i in range(1, 7):
            _relax_timeouts_for_sanitizer(
                os.path.join(CONFIG_DIR, _get_generated_cfg(node_names, i))
            )

        # Rolling-restart nodes 1/2/3 so they pick up the relaxed
        # timeouts.  Only one node is down at a time, so quorum is kept.
        for node_obj in [node1, node2, node3]:
            cfg_name = _get_generated_cfg(node_names, node_id(node_obj))
            node_obj.stop_clickhouse()
            node_obj.copy_file_to_container(
                os.path.join(CONFIG_DIR, cfg_name),
                "/etc/clickhouse-server/config.d/" + cfg_name,
            )
            node_obj.start_clickhouse()

        for n in [node1, node2, node3]:
            keeper_utils.wait_until_connected(cluster, n, timeout=60.0)

    # Identify the current leader and the two followers dynamically so the
    # test does not depend on which node wins the initial election.
    leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
    followers = [n for n in [node1, node2, node3] if n != leader]

    # Step 1: Add node4 to the cluster.
    # node4 creates peer objects for all three existing members.
    # node4's config omits use_cluster=false so ClickHouse can connect to the
    # existing Keeper cluster (node1/2/3) during startup, enabling async Keeper
    # initialisation and allowing start_clickhouse() to return before node4
    # reaches quorum.  The rcfg command then adds node4, the leader starts
    # sending heartbeats, and node4's Keeper completes initialisation in the
    # background.

    # Retrieve the generated config filename for node4 from CONFIG_DIR.
    cfg4 = _get_generated_cfg(node_names, 4)
    start_keeper(node4, cfg4)
    time.sleep(4)
    result = send_rcfg(
        cluster,
        leader,
        {
            "actions": [
                {
                    "add_members": [
                        {
                            "id": 4,
                            "endpoint": f"{node_names[4]}:9234",
                            "priority": 1,
                        }
                    ]
                }
            ]
        },
        timeout_sec=120,
    )
    assert result["status"] == "ok", f"Failed to add {node_names[4]}: {result}"
    keeper_utils.wait_until_connected(cluster, node4, timeout=60.0)

    # Step 2: Remove the first follower.
    # It receives `leave_cluster_req`, sets `steps_to_down_ = 2`, and after
    # 2 election timeouts calls `cancel_schedulers()`, which calls
    # `peer::shutdown()` on all its peer objects, setting `hb_task_ = null`.
    follower1 = followers[0]
    result = send_rcfg(
        cluster,
        leader,
        {"actions": [{"remove_members": [node_id(follower1)]}]},
        timeout_sec=30,
    )
    assert result["status"] == "ok", f"Failed to remove {follower1.name}: {result}"

    # Wait for the cluster to stabilize after the removal.
    time.sleep(2)

    remaining = [n for n in [node1, node2, node3, node4] if n != follower1]
    for n in remaining:
        keeper_utils.wait_until_connected(cluster, n, timeout=60.0)

    # Re-discover the leader after the membership change.
    leader = keeper_utils.get_leader(cluster, remaining)

    # Step 3: Add node5 to the cluster.  node1/3/4 are active at this point,
    # so node5 can connect to them during startup (async Keeper init).
    cfg5 = _get_generated_cfg(node_names, 5)
    start_keeper(node5, cfg5)
    time.sleep(4)
    result = send_rcfg(
        cluster,
        leader,
        {
            "actions": [
                {
                    "add_members": [
                        {
                            "id": 5,
                            "endpoint": f"{node_names[5]}:9234",
                            "priority": 1,
                        }
                    ]
                }
            ]
        },
        timeout_sec=120,
    )
    assert result["status"] == "ok", f"Failed to add {node_names[5]}: {result}"
    keeper_utils.wait_until_connected(cluster, node5, timeout=60.0)

    # Step 4: Remove the second follower.
    # Same `cancel_schedulers()` / `peer::shutdown()` sequence fires.
    follower2 = followers[1]
    result = send_rcfg(
        cluster,
        leader,
        {"actions": [{"remove_members": [node_id(follower2)]}]},
        timeout_sec=30,
    )
    assert result["status"] == "ok", f"Failed to remove {follower2.name}: {result}"

    time.sleep(2)

    active_nodes = [leader, node4, node5]
    for n in active_nodes:
        keeper_utils.wait_until_connected(cluster, n, timeout=60.0)

    # Re-discover the leader after the membership change.
    leader = keeper_utils.get_leader(cluster, active_nodes)

    # Step 5: Add node6 to the cluster.  node1/4/5 are active at this point,
    # so node6 can connect to them during startup (async Keeper init).
    cfg6 = _get_generated_cfg(node_names, 6)
    start_keeper(node6, cfg6)
    time.sleep(4)
    result = send_rcfg(
        cluster,
        leader,
        {
            "actions": [
                {
                    "add_members": [
                        {
                            "id": 6,
                            "endpoint": f"{node_names[6]}:9234",
                            "priority": 1,
                        }
                    ]
                }
            ]
        },
        timeout_sec=120,
    )
    assert result["status"] == "ok", f"Failed to add {node_names[6]}: {result}"
    keeper_utils.wait_until_connected(cluster, node6, timeout=60.0)

    # Step 6: Yield leadership so that node4, node5, or node6 wins the next
    # election.  ClickHouse rcfg does not allow removing the current leader
    # directly, so we first force it to step down via the `ydld` 4lw command.
    # That command triggers become_leader() on the winning replacement node,
    # which is exactly what triggers the bug: without the fix the stale
    # hb_task_ = null left by peer::shutdown() causes a null pointer dereference.
    keeper_utils.send_4lw_cmd(cluster, leader, cmd="ydld")

    # Give the election time to complete before checking for the new leader.
    time.sleep(2)

    keeper_utils.wait_nodes(cluster, [leader, node4, node5, node6])



def _get_generated_cfg(node_names: dict, node_idx: int) -> str:
    """Return the generated config filename for a given node index.

    The suffix is embedded in the node name, so we can recover it without
    storing it separately.
    """
    suffix = node_names[node_idx].split("_", 1)[1]
    return f"enable_keeper{node_idx}_{suffix}.xml"
