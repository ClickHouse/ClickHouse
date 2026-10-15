#!/usr/bin/env bash
# Tags: no-parallel, zookeeper, no-replicated-database
# Tag no-parallel: uses fail points which affect the whole server.
# Tag no-replicated-database: this test creates its own `Replicated` database
# explicitly, so it must not also run under the implicit replicated-database
# test wrapper (which would nest databases and rename the engine).
#
# Regression test for the `Replicated`-database rollback gap on
# https://github.com/ClickHouse/ClickHouse/pull/104822. For a plain MergeTree
# table inside a `Replicated` database, the durable metadata commit lives in
# ZooKeeper and cannot be rolled back. If the in-memory metadata is reverted to
# the old columns while the rename mutation stays registered, a background merge
# runs the rename against the old schema snapshot and silently replaces the
# renamed column's data with defaults (the #80648 data loss). The fix keeps the
# table in the consistent successful-alter state instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

REP_DB="${CLICKHOUSE_DATABASE}_rep"
ZK_PATH="/test/${CLICKHOUSE_DATABASE}/04311"

# Silence the distributed-DDL status rows so the reference only contains the
# query results we are asserting on.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"

$CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS ${REP_DB} SYNC"
$CLICKHOUSE_CLIENT --query="CREATE DATABASE ${REP_DB} ENGINE = Replicated('${ZK_PATH}', 'shard1', 'replica1')"

$CLICKHOUSE_CLIENT --query="
    CREATE TABLE ${REP_DB}.t (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO ${REP_DB}.t VALUES (1, 'hello'), (2, 'world');

    SYSTEM ENABLE FAILPOINT mt_alter_throw_after_mutation_registered;
"

# The failpoint fires after the rename mutation was registered but before the
# in-memory metadata is published. The ALTER must throw. `alter_sync = 0` so we
# do not block on the (now harmless) registered mutation.
set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE ${REP_DB}.t RENAME COLUMN d TO d1 SETTINGS alter_sync = 0" 2>/dev/null
alter_status=$?
set -e

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_after_mutation_registered"

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT --query="DROP DATABASE IF EXISTS ${REP_DB} SYNC"
    exit 1
fi

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
