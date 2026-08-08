#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: uses fail points which affect the whole server.
# Tag no-replicated-database: this test uses `DETACH TABLE` + `ATTACH TABLE`
# to force `loadMutations` to re-run, but `DETACH TABLE` is rejected with
# `INCORRECT_QUERY` under `Replicated` databases. Under `Replicated` the
# durable consistency invariant exercised here is preserved through a
# different code path: when the failpoint fires after registration but the
# durable metadata commit cannot be rolled back, the mutation entry stays
# registered and the rename mutation file stays on disk so reload finds the
# matching pair.
#
# Regression test for the post-registration rollback path on
# https://github.com/ClickHouse/ClickHouse/pull/104822.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_post_registration_rollback;

    CREATE TABLE t_post_registration_rollback (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0;

    INSERT INTO t_post_registration_rollback VALUES (1, 'hello'), (2, 'world');

    SYSTEM ENABLE FAILPOINT mt_alter_throw_after_mutation_registered;
"

# The ALTER must throw because the failpoint fires after the rename mutation
# was registered but before `setProperties` updated in-memory metadata.
set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE t_post_registration_rollback RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" 2>/dev/null
alter_status=$?
set -e

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_after_mutation_registered"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_post_registration_rollback"
    exit 1
fi

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_after_mutation_registered"

# The current process still serves the original column; the in-memory
# rollback under `currently_processing_in_background_mutex` reverted
# `setProperties`, and the matching durable rollback reverted the on-disk
# metadata.
$CLICKHOUSE_CLIENT --query="SELECT id, d FROM t_post_registration_rollback ORDER BY id"

set +e
$CLICKHOUSE_CLIENT --query="SELECT d1 FROM t_post_registration_rollback" 2>/dev/null
select_d1_status=$?
set -e
if [ "$select_d1_status" -eq 0 ]; then
    echo "FAIL: column 'd1' is visible after rolled-back ALTER"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_post_registration_rollback"
    exit 1
fi
echo "d1 not visible OK"

# DETACH + ATTACH reconstructs the storage and re-runs `loadMutations`. If
# the catch handler had failed to unregister and remove the rename mutation
# file, `loadMutations` would re-register a rename targeting a column that
# the rolled-back durable metadata no longer contains.
$CLICKHOUSE_CLIENT --query="DETACH TABLE t_post_registration_rollback"
$CLICKHOUSE_CLIENT --query="ATTACH TABLE t_post_registration_rollback"

orphan_count=$($CLICKHOUSE_CLIENT --query="
    SELECT count()
    FROM system.mutations
    WHERE database = currentDatabase()
      AND table = 't_post_registration_rollback'
")

if [ "$orphan_count" != "0" ]; then
    echo "FAIL: orphaned mutation persisted across DETACH/ATTACH (count=$orphan_count)"
    $CLICKHOUSE_CLIENT --query="SELECT mutation_id, command FROM system.mutations WHERE database = currentDatabase() AND table = 't_post_registration_rollback'"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_post_registration_rollback"
    exit 1
fi
echo "no orphaned mutation after DETACH/ATTACH OK"

# After the failed ALTER and the cleanup the table still exposes the
# original column `d` and a fresh ALTER must succeed normally.
$CLICKHOUSE_CLIENT --query="SELECT id, d FROM t_post_registration_rollback ORDER BY id"

$CLICKHOUSE_CLIENT --query="
    ALTER TABLE t_post_registration_rollback RENAME COLUMN d TO d1 SETTINGS alter_sync = 2;
    SELECT id, d1 FROM t_post_registration_rollback ORDER BY id;
    DROP TABLE t_post_registration_rollback;
"
