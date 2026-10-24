#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# A nested `SQL SECURITY DEFINER` view's settings profile is part of the rows it can return. If it
# changes, an `IF CHANGED` watermark made under the old settings must not skip the next refresh.
#
# The setting the definer changes must be one the outer query context does not change itself:
# `getSQLSecurityOverriddenContext` applies the outer context's changed settings on top of the
# definer's profile, so a setting that the server's `default` profile already sets (the test
# environment sets `max_rows_to_read`, for example) never reaches the view's context at all.
# Both values are far above what the view reads, so the refresh itself always succeeds and only
# the change of the definer's effective settings can make the second refresh run.
definer="definer_04909_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer} SETTINGS max_rows_to_read_leaf = 1000;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    CREATE VIEW v DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src ORDER BY x;
    -- Reports the value the definer's profile actually has inside a view's context, to tell a
    -- missing invalidation apart from an outer context that overrode the setting.
    CREATE VIEW effective_setting DEFINER = ${definer} SQL SECURITY DEFINER
        AS SELECT toUInt64(getSetting('max_rows_to_read_leaf'));
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY x AS SELECT x FROM v;
"

# Keep the two polling phases comfortably below the Fast test's 60-second timeout.
for _ in {1..30}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$initial" -eq 2 ] && break
    sleep 0.5
done

before=$($CLICKHOUSE_CLIENT -q "SELECT * FROM effective_setting")

# The source data is unchanged, but the nested view's effective settings changed. Without
# invalidating the old watermark the refresh is skipped and `mv` remains at two rows.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS max_rows_to_read_leaf = 2000"

after=$($CLICKHOUSE_CLIENT -q "SELECT * FROM effective_setting")

for _ in {1..30}
do
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$rows" -eq 4 ] && break
    sleep 0.5
done

[ "$initial" -eq 2 ] && [ "$rows" -eq 4 ] && echo "definer settings invalidate watermark: yes" || echo "definer settings invalidate watermark: no ($initial -> $rows, effective setting ${before} -> ${after})"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE v;
    DROP TABLE effective_setting;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
