#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: keeps the server-wide observables stable while the mixed
# ALTER is exercised.
# Tag no-replicated-database: a `Replicated` database rejects a mixed
# replicated + non-replicated ALTER up front (QUERY_IS_PROHIBITED), so the
# code path under test is only reachable on a plain (Atomic) database.
#
# Regression test for the metadata-validation-ordering gap on
# https://github.com/ClickHouse/ClickHouse/pull/104822: a mixed
# `RENAME COLUMN ..., MODIFY SETTING ...` where the new MergeTree setting makes
# an existing projection invalid (here `allow_part_offset_column_in_projections`
# is turned off while a `_part_offset` projection exists). The setting-dependent
# `checkProperties` validation must run before the durable metadata commit, so
# the ALTER is rejected with no durable change.
#
# Before the fix the durable metadata was committed first and `setProperties`
# threw afterwards, entering the rollback path and logging "may be inconsistent
# until restart" (the durable commit was ahead of the in-memory state, the
# #80648 data-loss class). On a non-replicated database the durable rollback
# then self-heals, so the only deterministic signal is that the dangerous
# rollback path was entered at all; the test asserts it was not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_proj_setting_validation;

    CREATE TABLE t_proj_setting_validation
    (
        id UInt64,
        d String DEFAULT '',
        PROJECTION p (SELECT id, _part_offset ORDER BY id)
    )
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, allow_part_offset_column_in_projections = 1;

    INSERT INTO t_proj_setting_validation VALUES (1, 'hello'), (2, 'world');
"

# The mixed ALTER makes the `_part_offset` projection invalid under the new
# setting, so it must throw. The rename of `d` is otherwise valid (the
# projection does not reference `d`), which forces the failure into the
# setting-dependent `checkProperties` step rather than the up-front CREATE-AST
# validation. A unique query_id scopes the text_log assertion below.
alter_query_id="04313_${CLICKHOUSE_DATABASE}_$(date +%s%N)"

set +e
$CLICKHOUSE_CLIENT --query_id="$alter_query_id" --query="ALTER TABLE t_proj_setting_validation RENAME COLUMN d TO d1, MODIFY SETTING allow_part_offset_column_in_projections = 0 SETTINGS alter_sync = 2" 2>/dev/null
alter_status=$?
set -e

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; invalid projection metadata was not rejected"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_proj_setting_validation"
    exit 1
fi

# Durable metadata must not have been committed: the column is still `d`.
$CLICKHOUSE_CLIENT --query="SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_proj_setting_validation' ORDER BY name"

# The setting must not have changed durably: it is still 1 in SHOW CREATE.
$CLICKHOUSE_CLIENT --query="SELECT extract(create_table_query, 'allow_part_offset_column_in_projections = \\d') FROM system.tables WHERE database = currentDatabase() AND table = 't_proj_setting_validation'"

# The data of the (un-renamed) column must be intact and readable.
$CLICKHOUSE_CLIENT --query="SELECT id, d FROM t_proj_setting_validation ORDER BY id"

# A merge must not silently corrupt the column: force one and re-check.
$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE t_proj_setting_validation FINAL"
$CLICKHOUSE_CLIENT --query="SELECT id, d FROM t_proj_setting_validation ORDER BY id"

# The validation must have happened before the durable commit, so the rejected
# ALTER must NOT have entered the post-commit rollback path. Assert the
# table did not log "may be inconsistent until restart" for this ALTER (it does
# before the fix). Scope by the ALTER's query_id so the check is deterministic.
$CLICKHOUSE_CLIENT --query="SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --query="
    SELECT count()
    FROM system.text_log
    WHERE query_id = '$alter_query_id'
      AND message LIKE '%may be inconsistent until restart%'
"

# A subsequent valid ALTER (rename only) must still succeed and apply.
$CLICKHOUSE_CLIENT --query="
    ALTER TABLE t_proj_setting_validation RENAME COLUMN d TO d1 SETTINGS alter_sync = 2;
    SELECT id, d1 FROM t_proj_setting_validation ORDER BY id;
    DROP TABLE t_proj_setting_validation;
"
