---
description: 'Design for ZooKeeper-compatible ClickHouse Keeper leader election metrics'
sidebar_label: 'Keeper Leader Metrics Design'
sidebar_position: 56
slug: /development/keeper-leader-metrics-design
title: 'ClickHouse Keeper Leader Metrics Design'
doc_type: 'reference'
---

# ClickHouse Keeper Leader Metrics Design {#clickhouse-keeper-leader-metrics-design}

## Goal {#goal}

Expose ZooKeeper-compatible leader election metrics through ClickHouse Keeper's `mntr` four-letter command:

- `zk_leader_uptime`
- `zk_sum_election_time`
- `zk_cnt_election_time`
- `zk_sum_leader_unavailable_time`
- `zk_cnt_leader_unavailable_time`

This change targets `mntr` compatibility. It does not add duplicate `Keeper*` metrics to `system.asynchronous_metrics` or Prometheus.

## Metric contract {#metric-contract}

All durations use a monotonic clock and are emitted as integer milliseconds. Values are local to one Keeper process and reset when that process restarts.

| Metric | Contract |
|---|---|
| `zk_leader_uptime` | Elapsed time since current node received NuRaft's `BecomeLeader` callback. Emit only on current leader. |
| `zk_sum_election_time` | Sum of sampled-start election durations won by current process. |
| `zk_cnt_election_time` | Number of sampled-start elections won by current process. |
| `zk_sum_leader_unavailable_time` | Sum of sampled no-leader windows completed by the current leader. |
| `zk_cnt_leader_unavailable_time` | Number of sampled no-leader windows completed by the current leader. |

`zk_sum_election_time`, `zk_cnt_election_time`, `zk_sum_leader_unavailable_time`, and `zk_cnt_leader_unavailable_time` sample `isLeaderAlive` once per configured `heart_beat_interval_ms`, but never more frequently than every 100 milliseconds. A poll that sees no live leader starts both local timers. The current leader completes election time at NuRaft `BecomeLeader` and leader-unavailability time when a poll sees its live leader state. A poll that sees a remote leader discards both local timers. This includes startup after NuRaft starts.

The metric starts are deliberately approximate. A start can be delayed by up to one effective polling interval, and a no-leader window shorter than that interval can be missed. Election completion is exact at `BecomeLeader`; leader-unavailability completion has the same polling error. The metric does not use `isServerActive`, so Keeper initialization and force recovery do not themselves extend a window.

`zk_leader_uptime` measures Raft leadership. The `mntr` command emits it only after Keeper is active, so it can include a short pre-serving interval after leadership is established.

`srst` resets the four cumulative metrics but does not reset `zk_leader_uptime`.

## Design {#design}

### Leader-availability polling boundary {#leader-availability-polling-boundary}

After NuRaft starts, Keeper schedules a poll once per configured `heart_beat_interval_ms`, clamped to a 100-millisecond minimum interval. The poll reads NuRaft's existing `is_leader_alive` and `is_leader` state. No NuRaft source change is required.

A poll that sees no live leader starts local election and leader-unavailability windows if none are active. `BecomeLeader` completes the election window. A later poll that sees a live local leader completes the leader-unavailability window. A poll that sees a remote leader discards both local windows. This reproduces ZooKeeper's winner-only reporting shape, but not its exact event boundaries.

### Keeper metric state {#keeper-metric-state}

Add a small `KeeperLeaderMetrics` state object owned by `KeeperServer`.

It stores running monotonic election and leader-unavailability timers plus their cumulative sums/counts under one mutex. `zk_leader_uptime` retains its separate atomic timestamp.

Expose an immutable snapshot through `Keeper4LWInfo`. `MonitorCommand::run` only renders snapshot fields; it owns no state or timing behavior.

### Callback wiring {#callback-wiring}

`KeeperServer::callbackFunc` starts `zk_leader_uptime` and completes the sampled-start election timer at `BecomeLeader`. It clears `zk_leader_uptime` at `BecomeFollower`. The leader-unavailability timer is completed by the periodic poll.

## Files to inspect and change {#files-to-inspect-and-change}

| File | Role |
|---|---|
| `src/Coordination/FourLetterCommand.cpp` | `MonitorCommand::run` emits `mntr` fields. |
| `src/Coordination/KeeperServer.cpp` | Schedule the poll and maintain election and leader-unavailability state. |
| `src/Coordination/KeeperServer.h` | Own polling and metric state. |
| `src/Coordination/KeeperDispatcher.h` | Reset cumulative metrics through `srst`. |
| `src/Coordination/Keeper4LWInfo.h` | Carry immutable metrics from `KeeperServer` to four-letter commands. |
| `tests/integration/test_keeper_four_word_command/test.py` | Extend `mntr` compatibility coverage. |
| `docs/guides/oss/deployment-and-scaling/keeper/index.mdx` | Document new `mntr` fields. |

## Alternatives rejected {#alternatives-rejected}

### Exact NuRaft lifecycle callbacks {#exact-nuraft-lifecycle-callbacks}

An exact metric needs a NuRaft election-start callback and a leader-ready callback. This design defers that change because ClickHouse cannot wait for an upstream NuRaft merge. The polling metric is intentionally kept as an approximation until such callbacks are available.

### Asynchronous metrics only {#asynchronous-metrics-only}

`system.asynchronous_metrics` is periodic. It can miss event boundaries and uses `Keeper*` naming, while requested names are ZooKeeper `mntr` compatibility keys. Adding both surfaces now duplicates public API without requirement.

### Profile events {#profile-events}

Profile events do not represent running leader uptime and do not model paired lifecycle timers naturally.

## Test plan {#test-plan}

1. Extend `test_cmd_mntr` to assert field presence and numeric types on leader.
2. Add three-node failover coverage that waits for a new leader and asserts nonnegative completed election and unavailable counters.
3. Verify that `srst` resets all four cumulative values.
4. Run the targeted Keeper four-letter-command integration test.

## Upstream NuRaft assessment {#upstream-nuraft-assessment}

NuRaft has no built-in election-duration or leader-unavailability metrics, and its callback API has no election-start or leader-ready event.

- [NuRaft issue 359](https://github.com/eBay/NuRaft/issues/359) asks for monitoring data. A NuRaft maintainer recommends exporting application metrics through callbacks, the state machine, or the log store. The maintainer states that eBay's Raft metrics live in an upper layer and are not open sourced.
- [NuRaft issue 247](https://github.com/eBay/NuRaft/issues/247) confirms the existing initialization-time callback model. It does not propose a new asynchronous callback facility.
- [NuRaft issue 216](https://github.com/eBay/NuRaft/issues/216) confirms that one callback function handles all event types and is configured during initialization.

No upstream issue or pull request found a plan for these lifecycle metrics. The upstream design expects the embedding application to own metric values, but a missing generic lifecycle boundary can justify a new callback event.

## Delivery scope and upstream plan {#delivery-scope-and-upstream-plan}

Current delivery scope is `zk_leader_uptime`, `zk_sum_election_time`, `zk_cnt_election_time`, `zk_sum_leader_unavailable_time`, and `zk_cnt_leader_unavailable_time`. The cumulative metrics use existing NuRaft state polling and require no NuRaft source change.

Exact boundaries are a future upstream NuRaft improvement:

1. Propose a generic election-start callback emitted after a valid election timeout and immediately before pre-vote or vote begins. It must include retries and failed terms without imposing a metric implementation on NuRaft.
2. Propose a leader-ready callback that identifies completion of NuRaft's mandatory protocol work for a new leader, not application readiness or arbitrary Raft topology changes.
3. Carry the same focused callback implementation in ClickHouse's `contrib/NuRaft` while the upstream pull request is under review. ClickHouse delivery must not wait for upstream merge; the upstream pull request is synchronization work, not a dependency.
4. Replace the polling starts and the sampled leader-unavailability end with these callbacks after that implementation is available.

States that cannot begin an election, such as NuRaft catch-up, snapshot receive, out-of-log-range, and learner mode, remain visible to the polling metric only when they coincide with no live leader. Keeper force recovery does not itself start or extend a sampled window.

## Main limitation {#main-limitation}

Metrics are not cluster-persistent. After failover, new leader reports its own process history, not history held by former leader. This matches normal per-node monitoring semantics and must be documented for alerting and dashboard aggregation.
