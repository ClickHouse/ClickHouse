#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: uses fail points which affect the whole server.
# Tag no-replicated-database: this test forces the non-replicated durable
# rollback path (durable metadata is rolled back via `alterTable`), which
# does not exist for `Replicated` databases, and it uses `DETACH`/`ATTACH`
# (rejected under `Replicated`) to re-run `loadMutations`.
#
# Regression test for the durable-rollback-failure path on
# https://github.com/ClickHouse/ClickHouse/pull/104822. When the in-memory
# metadata commit fails AND the durable metadata rollback then fails too, the
# rename mutation file must not be removed before the durable rollback
# succeeds. Otherwise durable metadata stays at `new_metadata` (renamed column)
# with no rename mutation to convert old parts, and on reload the renamed
# column reads back default values, reopening the #80648 data loss.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_durable_rollback_failure;

    CREATE TABLE t_durable_rollback_failure (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_durable_rollback_failure VALUES (1, 'hello'), (2, 'world');

    -- The first failpoint forces the in-memory commit to fail (after the rename
    -- mutation was registered and durable metadata committed to new_metadata).
    -- The second failpoint forces the durable metadata rollback to fail too, so
    -- durable metadata stays at new_metadata.
    SYSTEM ENABLE FAILPOINT mt_alter_throw_after_mutation_registered;
    SYSTEM ENABLE FAILPOINT mt_alter_throw_in_durable_rollback;
"

# The ALTER must throw: both the in-memory commit and the durable rollback fail.
set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE t_durable_rollback_failure RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" 2>/dev/null
alter_status=$?
set -e

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_after_mutation_registered"
$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_in_durable_rollback"

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoints did not fire"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_durable_rollback_failure"
    exit 1
fi

# Durable metadata could not be rolled back, so the server converges to the
# successful-alter state: the renamed column d1 is published and the rename
# mutation stays registered so a merge converts old parts.
$CLICKHOUSE_CLIENT --query="SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_durable_rollback_failure' ORDER BY name"

# There must still be exactly one rename mutation: the only conversion record
# was not orphaned. Before the fix it was removed before the durable rollback,
# so this would be 0.
$CLICKHOUSE_CLIENT --query="
    SELECT count()
    FROM system.mutations
    WHERE database = currentDatabase()
      AND table = 't_durable_rollback_failure'
"

# The data must be readable through the renamed column right away.
$CLICKHOUSE_CLIENT --query="SELECT id, d1 FROM t_durable_rollback_failure ORDER BY id"

# DETACH + ATTACH reconstructs the storage and re-runs loadMutations. This is
# the crux: if the rename mutation file had been removed while durable metadata
# stayed at new_metadata, loadMutations would load the renamed column with no
# rename mutation and the data would read back as default values.
$CLICKHOUSE_CLIENT --query="DETACH TABLE t_durable_rollback_failure"
$CLICKHOUSE_CLIENT --query="ATTACH TABLE t_durable_rollback_failure"

$CLICKHOUSE_CLIENT --query="SELECT id, d1 FROM t_durable_rollback_failure ORDER BY id"

# A merge must materialize the rename and keep the data intact.
$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE t_durable_rollback_failure FINAL"
$CLICKHOUSE_CLIENT --query="SELECT id, d1 FROM t_durable_rollback_failure ORDER BY id"

$CLICKHOUSE_CLIENT --query="DROP TABLE t_durable_rollback_failure"
