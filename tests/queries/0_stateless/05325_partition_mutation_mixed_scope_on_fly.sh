#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
#
# `no-replicated-database` / `no-shared-merge-tree`: `SYSTEM STOP MERGES` must hold on the only
# replica that could execute the mutation, so that it stays pending.
#
# A mutation entry can mix commands scoped to different partitions. A command applied on the fly
# must only be applied to the parts of its own partitions, not to those of the whole entry:
# `CLEAR COLUMN` has no predicate that would filter out the rows of other partitions.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS t_05325;
CREATE TABLE t_05325 (p UInt8, c String, d String, x UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO t_05325 VALUES (1, 'a', 'aa', 0), (2, 'b', 'bb', 0);
SYSTEM STOP MERGES t_05325;

-- alter_sync = 0: merges and mutations are stopped, so the default would wait forever.
ALTER TABLE t_05325 CLEAR COLUMN c IN PARTITION 1, CLEAR COLUMN d IN PARTITION 2 SETTINGS alter_sync = 0;
SELECT count(DISTINCT mutation_id) FROM system.mutations WHERE database = currentDatabase() AND table = 't_05325' AND NOT is_done;
SELECT p, c, d FROM t_05325 ORDER BY p;

SYSTEM START MERGES t_05325;
-- Barrier: waits for the pending CLEAR COLUMN as well.
ALTER TABLE t_05325 UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT p, c, d FROM t_05325 ORDER BY p;
DROP TABLE t_05325;

DROP TABLE IF EXISTS t_05325_r SYNC;
CREATE TABLE t_05325_r (p UInt8, c String, d String, x UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05325_r', '1') PARTITION BY p ORDER BY tuple();
INSERT INTO t_05325_r VALUES (1, 'a', 'aa', 0), (2, 'b', 'bb', 0);
SYSTEM STOP MERGES t_05325_r;

ALTER TABLE t_05325_r CLEAR COLUMN c IN PARTITION 1, CLEAR COLUMN d IN PARTITION 2 SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_05325_r PULL;
"

# `CLEAR COLUMN` is an `ALTER` with a metadata version, so it is applied on the fly only after the
# replica has executed the `ALTER_METADATA` entry. `SYSTEM SYNC REPLICA ... PULL` does not wait for it,
# and a full `SYSTEM SYNC REPLICA` would wait for the stopped mutation.
for _ in {1..600}
do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't_05325_r' AND type = 'ALTER_METADATA'") == 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --multiquery "
SELECT count(DISTINCT mutation_id) FROM system.mutations WHERE database = currentDatabase() AND table = 't_05325_r' AND NOT is_done;
SELECT p, c, d FROM t_05325_r ORDER BY p;

SYSTEM START MERGES t_05325_r;
ALTER TABLE t_05325_r UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT p, c, d FROM t_05325_r ORDER BY p;
DROP TABLE t_05325_r SYNC;
"
