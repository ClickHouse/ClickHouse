# Jepsen tests ClickHouse Keeper

A Clojure library designed to test ZooKeeper-like implementation inside ClickHouse.

## Test scenarios (workloads)

### CAS register

CAS Register has three operations: read number, write number, compare-and-swap number. This register is simulated as a single ZooKeeper node. Read transforms to ZooKeeper's `getData` request. Write transforms to the `set` request. Compare-and-swap implemented via `getData` + compare in code + `set` new value with `version` from `getData`.

In this test, we use a linearizable checker, so Jepsen validates that history was linearizable. One of the heaviest workloads.

Strictly requires `quorum_reads` to be true.

### Set

Set has two operations: add a number to set and read all values from set. This workload is simulated on a single ZooKeeper node with a string value that represents Clojure set data structure. Add operation very similar to compare-and-swap. We read string value from ZooKeeper node with `getData`, parse it to Clojure's set, add new value to the set and try to write it with the received version.

In this test, Jepsen validates that all successfully added values can be read. Generator for this workload performs only add operations until a timeout and after that tries to read set once.

### Unique IDs

In the Unique IDs workload we have only one operation: generate a new unique number. It's implemented using ZooKeeper's sequential nodes. For each generates request client just creates a new sequential node in ZooKeeper with a fixed prefix. After that cuts the prefix off from the returned path and parses the number from the rest part.

Jepsen checks that all returned IDs were unique.

### Counter

Counter workload has two operations: read counter value and add some number to the counter. Its implementation is quite weird. We add number `N` to the counter creating `N` sequential nodes in a single ZooKeeper transaction. Counter read implemented as `getChildren` ZooKeeper request and count of all returned nodes.

Jepsen checks that counter value lies in the interval of possible value. Strictly requires `quorum_reads` to be true.

### Total queue

Simulates an unordered queue with three operations: enqueue number, dequeue, and drain. Enqueue operation uses `create` request with node name equals to number. `Dequeue` operation is more interesting. We list (`getChildren`) all nodes and remember the parent node version. After that we choose the smallest one and prepare the transaction: `check` parent node version + set an empty value to parent node + delete smalled child node. Drain operation is just `getChildren` on the parent path.

Jepsen checks that all enqueued values were dequeued or drained. Duplicates are allowed because  Jepsen doesn't know the value of the unknown-status (`:info`) dequeue operation. So when we try to `dequeue` some element we should return it even if our delete transaction failed with `Connection loss` error.

### Linear queue

Same with the total queue, but without drain operation. Checks linearizability between enqueue and dequeue. Sometimes consume more than 10GB during validation even for very short histories.


## Nemesis

We use almost all standard nemeses with small changes for our storage.

### Random node killer (random-node-killer)

Sleep 5 seconds, kills random node, sleep for 5 seconds, and starts it back.

### All nodes killer (all-nodes-killer)

Kill all nodes at once, sleep for 5 seconds, and starts them back.

### Simple partitioner (simple-partitioner)

Partition one node from others using iptables. No one can see the victim and the victim cannot see anybody.

### Random node stop (random-node-hammer-time)

Send `SIGSTOP` to the random node. Sleep 5 seconds. Send `SIGCONT`.

### All nodes stop (all-nodes-hammer-time)

Send `SIGSTOP` to all nodes. Sleep 5 seconds. Send `SIGCONT`.

### Logs corruptor (logs-corruptor)

Corrupts latest log (change one random byte) in `clickhouse_path/coordination/logs`. Restarts nodes.

### Snapshots corruptor (snapshots-corruptor)

Corrupts latest snapshot (change one random byte) in `clickhouse_path/coordination/snapshots`. Restarts nodes.

### Logs and snapshots corruptor  (logs-and-snapshots-corruptor)

Corrupts both the latest log and snapshot. Restarts node.

### Drop data corruptor (drop-data-corruptor)

Drop all data from `clickhouse_path/coordinator`. Restarts node.

### Bridge partitioner (bridge-partitioner)

Two nodes don't see each other but can see another node. The last node can see both.

### Blind node partitioner (blind-node-partitioner)

One of the nodes cannot see another, but they can see it.

### Blind others partitioner (blind-others-partitioner)

Two nodes don't see one node but it can see both.

## Usage

### Dependencies

- leiningen (https://leiningen.org/)
- clojure (https://clojure.org/)
- jvm

### Options for `lein run`

- `test` Run a single test.
- `test-all` Run all available tests from tests-set.
- `-w (--workload)` One of the workloads. Option for a single `test`.
- `--nemesis` One of nemeses. Option for a single `test`.
- `-q (--quorum)` Run test with quorum reads.
- `-r (--rate)` How many operations per second Jepsen will generate in a single thread.
- `-s (--snapshot-distance)` ClickHouse Keeper setting. How often we will create a new snapshot.
- `--stale-log-gap` ClickHosue Keeper setting. A leader will send a snapshot instead of a log to this node if it's committed index less than leaders - this setting value.
- `--reserved-log-items` ClickHouse Keeper setting. How many log items to keep after the snapshot.
- `--ops-per-key` Option for CAS register workload. Total ops that will be generated for a single register.
- `--lightweight-run` Run some lightweight tests without linearizability checks. Option for `tests-all` run.
- `--reuse-binary` Don't download clickhouse binary if it already exists on the node.
- `--clickhouse-source` URL to clickhouse `.deb`, `.tgz` or binary.
- `--time-limit` (in seconds) How long Jepsen will generate new operations.
- `--nodes-file` File with nodes for SSH. Newline separated.
- `--username` SSH username for nodes.
- `--password` SSH password for nodes.
- `--concurrency` How many threads Jepsen will use for concurrent requests.
- `--test-count` How many times to run a single test or how many tests to run from the tests set.


### Examples:

1. Run `Set` workload with `logs-and-snapshots-corruptor` ten times:

```sh
$ lein run test --nodes-file nodes.txt --username root --password '' --time-limit 30 --concurrency 50 -r 50 --workload set --nemesis logs-and-snapshots-corruptor  --clickhouse-source 'https://clickhouse-builds.s3.yandex.net/someurl/clickhouse-common-static_21.4.1.6321_amd64.deb' -q --test-count 10 --reuse-binary
```

2. Run ten random tests from `lightweight-run` with some custom Keeper settings:

``` sh
$ lein run test-all --nodes-file nodes.txt --username root --password '' --time-limit 30 --concurrency 50 -r 50 --snapshot-distance 100 --stale-log-gap 100 --reserved-log-items 10 --lightweight-run  --clickhouse-source 'someurl' -q --reuse-binary --test-count 10
```


## License

Copyright © 2021 ClickHouse, Inc.

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
