#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# An `additional_table_filters` entry must take part in the `REFRESH ... IF CHANGED` hashes only when
# it can apply to the rows the refresh reads. Two wrappers do not pass the caller's filters through:
# a `SQL SECURITY DEFINER` view runs its body without the invoker's `additional_table_filters`, and a
# read of a materialized view is pushed straight into the target table, which ignores the filters keyed
# by the target. Filters for those inner tables must not discard the watermark, otherwise these
# `APPEND` views would append a duplicate copy of unchanged rows.

refresher="refresher_05316_${CLICKHOUSE_DATABASE}"
view_definer="view_definer_05316_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${refresher};
    CREATE USER ${view_definer};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${refresher};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${view_definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE mv_src_target (x UInt64) ENGINE = MergeTree ORDER BY x;
    -- A merge changes the active part set, which moves the modification hash and makes an extra
    -- refresh run. That is correct behavior, but it makes the row counts below unpredictable.
    SYSTEM STOP MERGES src;
    SYSTEM STOP MERGES mv_src_target;
    INSERT INTO src VALUES (1);
    INSERT INTO mv_src_target VALUES (1);

    CREATE VIEW v_definer DEFINER = ${view_definer} SQL SECURITY DEFINER AS SELECT x FROM src;
    CREATE MATERIALIZED VIEW mv_src TO mv_src_target DEFINER = ${view_definer} SQL SECURITY DEFINER AS SELECT x FROM src;

    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv_through_definer_view REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${refresher} SQL SECURITY DEFINER AS SELECT count() AS cnt FROM v_definer;
    CREATE MATERIALIZED VIEW mv_through_mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt
        DEFINER = ${refresher} SQL SECURITY DEFINER AS SELECT count() AS cnt FROM mv_src;
"

counts()
{
    $CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM mv_through_definer_view) || ' ' || (SELECT count() FROM mv_through_mv)"
}

# Waits until both views have more than the given number of rows.
wait_for_more_than()
{
    local through_definer_view=$1 through_mv=$2
    for _ in {1..60}
    do
        read -r current_through_definer_view current_through_mv <<< "$(counts)"
        [ "$current_through_definer_view" -gt "$through_definer_view" ] && [ "$current_through_mv" -gt "$through_mv" ] && break
        sleep 0.5
    done
    echo "$current_through_definer_view $current_through_mv"
}

# Waits until the given view has more than the given number of rows.
wait_for_view()
{
    local view=$1 rows=$2 current
    for _ in {1..60}
    do
        current=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM ${view}")
        [ "$current" -gt "$rows" ] && break
        sleep 0.5
    done
    echo "$current"
}

# The first refresh always runs: there is no previous state to compare to.
initial=$(wait_for_more_than 0 0)

# Filters of the refreshing user for the table behind the `DEFINER` view and for the target of the
# materialized view, by bare and by qualified name. Neither applies to the rows the refreshes read.
# (A user profile takes the map as a string.)
$CLICKHOUSE_CLIENT -q "ALTER USER ${refresher} SETTINGS additional_table_filters = '{\\'src\\': \\'x = 2\\', \\'${CLICKHOUSE_DATABASE}.src\\': \\'x = 3\\', \\'mv_src_target\\': \\'x = 4\\', \\'${CLICKHOUSE_DATABASE}.mv_src_target\\': \\'x = 5\\'}'"
# A filter of the definer of the materialized view for its target table does not apply either.
$CLICKHOUSE_CLIENT -q "ALTER USER ${view_definer} SETTINGS additional_table_filters = '{\\'mv_src_target\\': \\'x = 6\\'}'"

sleep 3
after=$(counts)
[ "$initial" = "1 1" ] && [ "$after" = "1 1" ] && echo "non-applying filters keep the watermark: yes" || echo "non-applying filters keep the watermark: no ($initial -> $after)"

# Filters that do apply discard the watermark: one of the view definer for the table its body reads,
# and one of the refreshing user for the materialized view itself. This shows that the entries above
# are excluded rather than the whole setting being ignored.
$CLICKHOUSE_CLIENT -q "ALTER USER ${view_definer} SETTINGS additional_table_filters = '{\\'src\\': \\'x > 0\\'}'"
through_definer_view=$(wait_for_view mv_through_definer_view 1)
[ "$through_definer_view" -gt 1 ] && echo "a filter of the view definer invalidates the watermark: yes" || echo "a filter of the view definer invalidates the watermark: no ($through_definer_view)"

$CLICKHOUSE_CLIENT -q "ALTER USER ${refresher} SETTINGS additional_table_filters = '{\\'mv_src\\': \\'x > 0\\'}'"
through_mv=$(wait_for_view mv_through_mv 1)
[ "$through_mv" -gt 1 ] && echo "a filter for the materialized view invalidates the watermark: yes" || echo "a filter for the materialized view invalidates the watermark: no ($through_mv)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_through_definer_view SYNC;
    DROP TABLE mv_through_mv SYNC;
    DROP TABLE mv_src SYNC;
    DROP TABLE v_definer SYNC;
    DROP TABLE mv_src_target SYNC;
    DROP TABLE src SYNC;
    DROP USER ${refresher};
    DROP USER ${view_definer};
"
