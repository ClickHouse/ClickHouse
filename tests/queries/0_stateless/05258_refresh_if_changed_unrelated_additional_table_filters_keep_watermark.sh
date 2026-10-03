#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# The `REFRESH ... IF CHANGED` watermark is keyed on a hash of the view definition, which folds the
# settings of the refresh context that can change the rows the refresh `SELECT` produces. An
# `additional_table_filters` entry only applies to a table whose name matches its key, so an entry for
# a table the refresh never reads must not discard the watermark: these `APPEND` views would then
# append a duplicate copy of unchanged rows. One view reads the source directly, the other through a
# plain view, because the filters also apply to the tables read inside a view.

definer="definer_05258_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    -- The refresh runs as the definer, so it needs to write the inner tables of the views as well.
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE other (x UInt64) ENGINE = MergeTree ORDER BY x;
    -- A merge changes the active part set of the source table, which moves its modification hash and
    -- makes an extra refresh run. That is correct behavior, but it makes the row counts below
    -- unpredictable. (No backticks in this comment: the whole statement is a double-quoted shell
    -- string, where they would start a command substitution.)
    SYSTEM STOP MERGES src;
    INSERT INTO src VALUES (1);
    CREATE VIEW v AS SELECT x FROM src;
    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv_direct REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT count() AS cnt FROM src;
    CREATE MATERIALIZED VIEW mv_through_view REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT count() AS cnt FROM v;
"

counts()
{
    $CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_direct) || ' ' || (SELECT count() FROM mv_through_view)"
}

# Waits until both views have more than the given number of rows.
wait_for_more_than()
{
    local direct=$1 through_view=$2
    for _ in {1..60}
    do
        read -r current_direct current_through_view <<< "$(counts)"
        [ "$current_direct" -gt "$direct" ] && [ "$current_through_view" -gt "$through_view" ] && break
        sleep 0.5
    done
    echo "$current_direct $current_through_view"
}

# The first refresh always runs: there is no previous state to compare to.
initial=$(wait_for_more_than 0 0)

# Filters for tables that neither view reads, by bare and by qualified name. (A user profile takes the
# map as a string.) The sources are unchanged,
# so the following scheduled refreshes must still be skipped.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS additional_table_filters = '{\\'other\\': \\'x = 1\\', \\'${CLICKHOUSE_DATABASE}.other\\': \\'x = 2\\', \\'no_such_table\\': \\'x = 3\\'}'"

sleep 3
after=$(counts)
[ "$initial" = "1 1" ] && [ "$after" = "1 1" ] && echo "unrelated filters keep the watermark: yes" || echo "unrelated filters keep the watermark: no ($initial -> $after)"

# A filter for the source table does apply - directly and inside the view - so it discards the
# watermark of both views: the check above means unrelated entries are excluded rather than the
# whole setting being ignored.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS additional_table_filters = '{\\'other\\': \\'x = 1\\', \\'${CLICKHOUSE_DATABASE}.src\\': \\'x > 0\\'}'"
invalidated=$(wait_for_more_than 1 1)
[ "$invalidated" != "1 1" ] && [ "${invalidated% *}" -gt 1 ] && [ "${invalidated#* }" -gt 1 ] && echo "a filter for the source invalidates the watermark: yes" || echo "a filter for the source invalidates the watermark: no ($invalidated)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_direct SYNC;
    DROP TABLE mv_through_view SYNC;
    DROP TABLE v SYNC;
    DROP TABLE other SYNC;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
