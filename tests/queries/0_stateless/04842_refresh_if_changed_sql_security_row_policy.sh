#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

# Regression test for `REFRESH ... IF CHANGED` reading through a `SQL SECURITY DEFINER` view (issue
# #108713, PR #108721). The view's stored `SELECT` is executed under the view's own effective security
# context, so the rows the refresh reads are the rows the *definer* sees. A non-trivial `SELECT` row
# policy of that definer changes them while the table behind the view is untouched, so the source hash
# has to fail closed on it and every scheduled refresh must run. Sampling the hash under the refreshing
# view's context instead never saw the definer's policy: the source looked unchanged, refreshes were
# skipped, and the materialized view kept a result that no longer matched what reading the view returns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Users and row policies are server-wide, so make the names unique per run.
definer="definer_04842_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    CREATE ROW POLICY policy_04842 ON ${CLICKHOUSE_DATABASE}.src FOR SELECT USING x = 1 TO ${definer};
    -- A table with any row policy is unreadable by a user none of its policies mention, so give the
    -- refreshing view's own reader a literally always true one. Such a filter is deliberately not
    -- counted by the consistency check, so the definer's non-trivial policy is the only difference
    -- between the refresh context and the view's effective context.
    CREATE ROW POLICY policy_all_04842 ON ${CLICKHOUSE_DATABASE}.src FOR SELECT USING 1 TO CURRENT_USER;

    -- The definer sees only part of \`src\`, so what this view returns depends on a row policy that the
    -- refreshing view's own context cannot see.
    CREATE VIEW v DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src;
    -- APPEND mode: every refresh that actually runs appends one row.
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 SECOND IF CHANGED APPEND
        ENGINE = MergeTree ORDER BY cnt AS SELECT count() AS cnt FROM v;
"

# Wait for the first refresh (it always runs - there is no previous state to compare against).
for _ in {1..120}
do
    n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n" -ge 1 ] && break
    sleep 0.5
done

# `src` is unchanged, but the definer's row policy makes the source hash unavailable, so the refresh
# cannot prove the source is unchanged and must keep running: more rows appear.
for _ in {1..120}
do
    n2=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n2" -ge 3 ] && break
    sleep 0.5
done
[ "$n2" -ge 3 ] && echo "row policy on the definer keeps refreshing: yes" || echo "row policy on the definer keeps refreshing: no ($n2)"

# Without a row policy for the definer the source hash is available again, so refreshes of an unchanged
# source are skipped: the row count stops growing.
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY policy_04842, policy_all_04842 ON ${CLICKHOUSE_DATABASE}.src"
for _ in {1..120}
do
    before=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    sleep 3
    after=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$before" = "$after" ] && break
done
[ "$before" = "$after" ] && echo "without the policy an unchanged source is skipped: yes" || echo "without the policy an unchanged source is skipped: no ($before -> $after)"

# Changing `src` makes a scheduled refresh run again.
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (3)"
for _ in {1..120}
do
    n3=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM mv")
    [ "$n3" -gt "$after" ] && break
    sleep 0.5
done
[ "$n3" -gt "$after" ] && echo "changed source triggers a refresh: yes" || echo "changed source triggers a refresh: no ($n3)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv SYNC;
    DROP TABLE v;
    DROP TABLE src SYNC;
    DROP USER ${definer};
"
