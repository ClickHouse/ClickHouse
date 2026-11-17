#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# Tag no-parallel: uses fail points which affect the whole server.
# Tag no-replicated-database: the durable rollback path tested here is the
# non-replicated one (durable metadata is rolled back to the old settings),
# which does not exist for `Replicated` databases.
#
# Regression test for the settings-rollback gap on
# https://github.com/ClickHouse/ClickHouse/pull/104822: a mixed
# `RENAME COLUMN ..., MODIFY SETTING ...` whose mutation preparation throws
# before the durable metadata commit must not leave the new setting live
# in-memory while columns and durable metadata stay old.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# `min_bytes_for_wide_part` controls the on-INSERT part type and is read from
# the live in-memory storage settings, so it is a direct observable for whether
# `changeSettings` was rolled back. Start large so small inserts are Compact.
$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS t_settings_rollback;

    CREATE TABLE t_settings_rollback (id UInt64, d String DEFAULT '')
    ENGINE = MergeTree() ORDER BY id
    SETTINGS min_bytes_for_wide_part = 10000000000;

    INSERT INTO t_settings_rollback VALUES (1, 'hello');

    SYSTEM ENABLE FAILPOINT mt_throw_after_mutation_commit;
"

# The failpoint fires inside `prepareMutationEntry`, before the durable metadata
# commit. The ALTER must throw.
set +e
$CLICKHOUSE_CLIENT --query="ALTER TABLE t_settings_rollback RENAME COLUMN d TO d1, MODIFY SETTING min_bytes_for_wide_part = 0 SETTINGS alter_sync = 2" 2>/dev/null
alter_status=$?
set -e

$CLICKHOUSE_CLIENT --query="SYSTEM DISABLE FAILPOINT mt_throw_after_mutation_commit"

if [ "$alter_status" -eq 0 ]; then
    echo "FAIL: ALTER unexpectedly succeeded; failpoint did not fire"
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_settings_rollback"
    exit 1
fi

# Columns must still be the old ones (durable metadata was not committed).
$CLICKHOUSE_CLIENT --query="SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_settings_rollback' ORDER BY name"

# The new setting must have been rolled back: a fresh small insert must still
# produce a Compact part. Before the fix the setting leaked to 0 and the part
# would be Wide.
$CLICKHOUSE_CLIENT --query="INSERT INTO t_settings_rollback VALUES (2, 'world')"
$CLICKHOUSE_CLIENT --query="SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_settings_rollback' AND active"

# A fresh mixed ALTER must succeed and apply both the rename and the setting.
$CLICKHOUSE_CLIENT --query="
    ALTER TABLE t_settings_rollback RENAME COLUMN d TO d1, MODIFY SETTING min_bytes_for_wide_part = 0 SETTINGS alter_sync = 2;
    SELECT id, d1 FROM t_settings_rollback ORDER BY id;
    DROP TABLE t_settings_rollback;
"
