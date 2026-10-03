#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The `REFRESH ... IF CHANGED` watermark is keyed on a hash of the view definition, which folds the
# settings of the refresh context because they can change the rows the refresh `SELECT` produces. The
# query cache settings cannot: they only decide whether, where and for how long a result is cached. If
# they were folded in, a pure caching-policy edit to the definer's profile would discard the
# watermark, and this `APPEND` view would append a duplicate copy of unchanged rows.

definer="definer_05101_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    -- The refresh runs as the definer, so it needs to write the view's inner table as well.
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    -- A merge changes the active part set of the source table, which moves its modification hash and
    -- makes an extra refresh run. That is correct behavior, but it makes the row counts below
    -- unpredictable. (No backticks in this comment: the whole statement is a double-quoted shell
    -- string, where they would start a command substitution.)
    SYSTEM STOP MERGES src;
    INSERT INTO src VALUES (1);
    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT count() AS cnt FROM src;
"

# The first refresh always runs: there is no previous state to compare to.
for _ in {1..60}
do
    initial=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$initial" -ge 1 ] && break
    sleep 0.5
done

# A pure caching-policy edit to the definer's profile. The source is unchanged, so the following
# scheduled refreshes must still be skipped.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS use_query_cache = 1, query_cache_tag = 'tag_05101', query_cache_ttl = 300"

sleep 3
after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
[ "$initial" = "1" ] && [ "$after" = "1" ] && echo "query cache settings keep the watermark: yes" || echo "query cache settings keep the watermark: no ($initial -> $after)"

# A real change of the source still triggers a refresh, so the watermark is in use rather than ignored.
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (2)"
for _ in {1..60}
do
    changed=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$changed" -ge 2 ] && break
    sleep 0.5
done
[ "$changed" -ge 2 ] && echo "changed source triggers refresh: yes" || echo "changed source triggers refresh: no ($changed)"

# And a setting that can change the rows the refresh reads still discards the watermark, so the check
# above means the query cache settings are excluded rather than the whole settings fold being dead.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS use_query_cache = 1, query_cache_tag = 'tag_05101', query_cache_ttl = 300, max_rows_to_read_leaf = 100000"
for _ in {1..60}
do
    invalidated=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$invalidated" -gt "$changed" ] && break
    sleep 0.5
done
[ "$invalidated" -gt "$changed" ] && echo "a row-affecting setting invalidates the watermark: yes" || echo "a row-affecting setting invalidates the watermark: no ($invalidated)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
