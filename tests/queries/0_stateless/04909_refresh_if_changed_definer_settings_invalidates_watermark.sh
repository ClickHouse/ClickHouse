#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The `SQL SECURITY DEFINER` context is rebuilt for each refresh. If its settings profile changes,
# an `IF CHANGED` watermark made under the old settings must not skip the next refresh.
definer="definer_04909_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer} SETTINGS max_result_rows = 1, result_overflow_mode = 'break';
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    CREATE VIEW v DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src ORDER BY x;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY x AS SELECT x FROM v;
"

for _ in {1..120}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$initial" -eq 1 ] && break
    sleep 0.5
done

# The source data is unchanged, but the definer now reads two rows. Without invalidating the old
# watermark the refresh is skipped and `mv` remains at one row.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_result_rows = 2, result_overflow_mode = 'break'"
for _ in {1..120}
do
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$rows" -eq 3 ] && break
    sleep 0.5
done

[ "$initial" -eq 1 ] && [ "$rows" -eq 3 ] && echo "definer settings invalidate watermark: yes" || echo "definer settings invalidate watermark: no ($initial -> $rows)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE v;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
