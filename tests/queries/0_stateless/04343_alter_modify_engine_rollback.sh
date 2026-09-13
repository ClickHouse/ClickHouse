#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: uses a fail point, which affects the whole server.
# Tag no-replicated-database: forces the non-replicated durable rollback path
# (`alterTable(old_metadata)`), which does not exist for `Replicated` databases,
# and uses `DETACH`/`ATTACH`, which they reject.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_modify_engine_rollback;

    CREATE TABLE t_modify_engine_rollback (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_modify_engine_rollback VALUES (1, 100), (1, 200);

    SYSTEM ENABLE FAILPOINT mt_alter_throw_after_mutation_registered;
"

set +e
alter_error=$($CLICKHOUSE_CLIENT --query="
    SET allow_experimental_alter_modify_engine = 1;
    ALTER TABLE t_modify_engine_rollback ADD COLUMN w UInt32, MODIFY ENGINE = ReplacingMergeTree(v);
" 2>&1)
alter_status=$?
set -e

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_alter_throw_after_mutation_registered"

# The ALTER must fail through the injected POST-COMMIT path. Accepting any failure would let an
# unrelated pre-commit rejection satisfy the whole reference without ever rolling back.
if [ "$alter_status" -eq 0 ] || ! echo "$alter_error" | grep -q "FAULT_INJECTED"; then
    echo "FAIL: expected the injected post-commit failure, got status $alter_status: $alter_error"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_modify_engine_rollback"
    exit 1
fi

# The engine must be rolled back with the rest of the statement. The engine clause is
# rewritten in the stored CREATE query, so a rollback that only restores columns would
# leave ReplacingMergeTree on disk and activate it on the next load.
$CLICKHOUSE_CLIENT --query="
    SELECT extract(create_table_query, 'ENGINE = [A-Za-z]+')
    FROM system.tables WHERE database = currentDatabase() AND name = 't_modify_engine_rollback'
"

# The reload is where a surviving engine clause would take effect.
$CLICKHOUSE_CLIENT --query="DETACH TABLE t_modify_engine_rollback"
$CLICKHOUSE_CLIENT --query="ATTACH TABLE t_modify_engine_rollback"

$CLICKHOUSE_CLIENT --query="
    SELECT engine FROM system.tables
    WHERE database = currentDatabase() AND name = 't_modify_engine_rollback'
"

# Still plain MergeTree semantics: both rows survive a merge.
$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE t_modify_engine_rollback FINAL"
$CLICKHOUSE_CLIENT --query="SELECT count() FROM t_modify_engine_rollback"

$CLICKHOUSE_CLIENT --query="DROP TABLE t_modify_engine_rollback"
