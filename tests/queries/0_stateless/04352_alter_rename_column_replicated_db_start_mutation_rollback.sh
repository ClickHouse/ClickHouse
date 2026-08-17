#!/usr/bin/env bash
# Tags: no-parallel, zookeeper, no-replicated-database
# Tag no-parallel: uses fail points which affect the whole server.
# Tag no-replicated-database: this test creates its own `Replicated` database
# explicitly, so it must not also run under the implicit replicated-database
# test wrapper (which would nest databases and rename the engine).
#
# Companion of 04311 for the pre-registration failure point on
# https://github.com/ClickHouse/ClickHouse/pull/104822. `mt_alter_throw_in_start_mutation`
# throws before the rename mutation is registered, so the rollback handler enters with
# `mutation_registered == false` and must register the prepared mutation itself (its
# `mutation_*.txt` is already durable) before publishing `new_metadata`. 04311 enters the
# same handler with the mutation already registered; 04296 covers this failure point on a
# non-replicated database, where the durable metadata is reverted instead.
#
# A `Replicated` database commits table metadata to ZooKeeper, which cannot be rolled back,
# so the handler must converge forward. Reverting in memory while the rename mutation stays
# registered would let a merge run the rename against the old schema and reopen the #80648
# data loss.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

REP_DB="${CLICKHOUSE_DATABASE}_rep"
ZK_PATH="/test/${CLICKHOUSE_DATABASE}/04352"

# Silence the distributed-DDL status rows so the reference only contains the
# query results asserted on.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS ${REP_DB} SYNC"
$CLICKHOUSE_CLIENT --query="CREATE DATABASE ${REP_DB} ENGINE = Replicated('${ZK_PATH}', 'shard1', 'replica1')"

$CLICKHOUSE_CLIENT --query="
    CREATE TABLE ${REP_DB}.t (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO ${REP_DB}.t VALUES (1, 'hello'), (2, 'world');

    SYSTEM ENABLE FAILPOINT mt_alter_throw_in_start_mutation;
"

# The ALTER must throw. `alter_sync = 0` so the test does not block on the
# (now harmless) registered mutation.
set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE ${REP_DB}.t RENAME COLUMN d TO d1 SETTINGS alter_sync = 0" 2>/dev/null
alter_status=$?
set -e

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_in_start_mutation"

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS ${REP_DB} SYNC"
    exit 1
fi

# The handler entered with the mutation unregistered, so it must have registered
# the prepared entry itself. `system.mutations` reads the in-memory mutation map,
# so this pins that step before any reload could re-read the durable file. On the
# non-replicated path the same failure point leaves no mutation registered.
$CLICKHOUSE_CLIENT --query="SELECT count(), any(command) FROM system.mutations WHERE database = '${REP_DB}' AND table = 't'"

# The durable metadata commit (column `d1`) could not be rolled back, so the
# in-memory metadata must converge to it: column `d1` is visible and `d` is not.
$CLICKHOUSE_CLIENT --query="SELECT name FROM system.columns WHERE database = '${REP_DB}' AND table = 't' ORDER BY name"

# The data of the renamed column must be preserved (not replaced by defaults).
# A concurrent merge would have reopened #80648; force one and re-check.
$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE ${REP_DB}.t FINAL"
$CLICKHOUSE_CLIENT --query="SELECT id, d1 FROM ${REP_DB}.t ORDER BY id"

# Reload the table from the durable metadata and confirm the data is still
# intact after the registered rename mutation has had a chance to run.
$CLICKHOUSE_CLIENT --query="DETACH TABLE ${REP_DB}.t PERMANENTLY"
$CLICKHOUSE_CLIENT --query="ATTACH TABLE ${REP_DB}.t"
$CLICKHOUSE_CLIENT --query="SELECT id, d1 FROM ${REP_DB}.t ORDER BY id"

$CLICKHOUSE_CLIENT --query="DROP DATABASE ${REP_DB} SYNC"
