#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# A view's modification hash must not depend on settings that carry per-query diagnostics only.
# `RefreshTask` puts the attempt number into `log_comment`, so a hash that folds it in would differ
# between a first attempt and a retry, and a `REFRESH ... IF CHANGED APPEND` view would append a
# duplicate copy of unchanged rows after a retry.

$CLICKHOUSE_CLIENT -q "
    DROP VIEW IF EXISTS v_hash_settings_05054;
    DROP TABLE IF EXISTS t_hash_settings_05054;
    CREATE TABLE t_hash_settings_05054 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_hash_settings_05054 VALUES (1);
    CREATE VIEW v_hash_settings_05054 AS SELECT x FROM t_hash_settings_05054;
"

hash_of_view()
{
    $CLICKHOUSE_CLIENT "$@" -q "
        SELECT toString(modification_hash)
        FROM system.tables
        WHERE database = currentDatabase() AND name = 'v_hash_settings_05054'
    " | sed -n '1p'
}

first_attempt=$(hash_of_view --log_comment "refresh of v (attempt 1/3)")
retry_attempt=$(hash_of_view --log_comment "refresh of v (attempt 2/3)")

# The read path also drops the result limits and `extremes` from the view context (they apply to the
# outer query), so they must not move the hash either. `--extremes 1` makes the client print the
# extremes block after the row, hence the `sed -n '1p'` above.
limited=$(hash_of_view --log_comment "refresh of v (attempt 1/3)" --max_result_rows 1 --extremes 1)

[ -n "${first_attempt}" ] && echo 'hash is computed'
[ "${first_attempt}" = "${retry_attempt}" ] && echo 'log_comment does not change the hash'
[ "${first_attempt}" = "${limited}" ] && echo 'result limits do not change the hash'

# A real change of the source still moves the hash.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_hash_settings_05054 VALUES (2)"
after_insert=$(hash_of_view --log_comment "refresh of v (attempt 1/3)")
[ "${first_attempt}" != "${after_insert}" ] && echo 'an insert into the source changes the hash'

$CLICKHOUSE_CLIENT -q "
    DROP VIEW v_hash_settings_05054;
    DROP TABLE t_hash_settings_05054;
"
