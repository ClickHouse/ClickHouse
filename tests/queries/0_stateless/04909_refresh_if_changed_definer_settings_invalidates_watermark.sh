#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# A nested `SQL SECURITY DEFINER` view's settings profile is part of the rows it can return. If it
# changes, an `IF CHANGED` watermark made under the old settings must not skip the next refresh.
definer="definer_04909_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer} SETTINGS max_block_size = 1, max_rows_to_read = 1, read_overflow_mode = 'break';
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    CREATE VIEW v DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src ORDER BY x;
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY x AS SELECT x FROM v;
"

# Keep the two polling phases comfortably below the Fast test's 60-second timeout.
for _ in {1..30}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$initial" -eq 1 ] && break
    sleep 0.5
done

# The source data is unchanged, but the nested view's effective settings changed. Without
# invalidating the old watermark the refresh is skipped and `mv` remains at two rows.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_block_size = 1, max_rows_to_read = 2, read_overflow_mode = 'break'"
for _ in {1..30}
do
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$rows" -eq 4 ] && break
    sleep 0.5
done

[ "$initial" -eq 2 ] && [ "$rows" -eq 4 ] && echo "definer settings invalidate watermark: yes" || echo "definer settings invalidate watermark: no ($initial -> $rows)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE v;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
