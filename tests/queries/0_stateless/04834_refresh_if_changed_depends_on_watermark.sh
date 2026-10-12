#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.
# Regression test for the `DEPENDS ON` scheduler watermark with `REFRESH ... IF CHANGED` (PR #108713):
# a refresh that is skipped because the sources are unchanged must still consume the dependency
# refresh that triggered it (advance `last_success_dependencies`), otherwise the same dependency
# refresh keeps looking new and the view loops through immediate skipped refreshes instead of going
# back to waiting for the dependency.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS dep_mv SYNC;
    DROP TABLE IF EXISTS src_mv SYNC;
    DROP TABLE IF EXISTS src SYNC;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    -- The dependency: refreshes only when asked (the 1 HOUR period never fires within the test).
    CREATE MATERIALIZED VIEW src_mv REFRESH AFTER 1 HOUR APPEND
        ENGINE = MergeTree ORDER BY cnt AS SELECT count() AS cnt FROM src;
"

# Wait for the dependency's initial refresh, so that the dependent view created below can run its own
# initial refresh right away (a dependency that never refreshed satisfies nothing).
for _ in {1..120}
do
    n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM src_mv")
    [ "$n" -ge 1 ] && break
    sleep 0.5
done

# The dependent view reads from `src` (not from `src_mv`), so a refresh of `src_mv` triggers it while
# its own sources stay unchanged - exactly the case where `IF CHANGED` skips.
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW dep_mv REFRESH AFTER 1 SECOND IF CHANGED DEPENDS ON src_mv APPEND
        ENGINE = MergeTree ORDER BY c AS SELECT count() AS c FROM src;
"

# Wait for the dependent's initial refresh (always runs: no previous state to compare to) and for the
# scheduler to settle back into waiting for the dependency.
for _ in {1..120}
do
    n=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dep_mv")
    [ "$n" -ge 1 ] && break
    sleep 0.5
done
for _ in {1..120}
do
    s=$($CLICKHOUSE_CLIENT -q "SELECT status FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'dep_mv'")
    [ "$s" = "WaitingForDependencies" ] && break
    sleep 0.5
done
echo "initial refresh ran, then waiting for dependencies: $([ "$s" = "WaitingForDependencies" ] && echo yes || echo "no ($s)")"

# Refresh the dependency. This triggers the dependent view, whose sources are unchanged, so the
# triggered refresh must be skipped (no row appended) - and the skip must consume the dependency
# refresh: the view must settle back into WaitingForDependencies instead of looping through immediate
# skipped refreshes.
t0=$($CLICKHOUSE_CLIENT -q "SELECT last_refresh_time FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'dep_mv'")
sleep 1.5  # a new wall-clock second, so the skipped attempt is visible in last_refresh_time
$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW src_mv"
for _ in {1..120}
do
    t1=$($CLICKHOUSE_CLIENT -q "SELECT last_refresh_time FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'dep_mv'")
    [ "$t1" != "$t0" ] && break
    sleep 0.5
done
n2=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dep_mv")
echo "dependency-triggered refresh was skipped: $([ "$n2" = "1" ] && echo yes || echo "no ($n2)")"

for _ in {1..120}
do
    s=$($CLICKHOUSE_CLIENT -q "SELECT status FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'dep_mv'")
    [ "$s" = "WaitingForDependencies" ] && break
    sleep 0.5
done
t1=$($CLICKHOUSE_CLIENT -q "SELECT last_refresh_time FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'dep_mv'")
sleep 3
t2=$($CLICKHOUSE_CLIENT -q "SELECT last_refresh_time FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'dep_mv'")
s=$($CLICKHOUSE_CLIENT -q "SELECT status FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'dep_mv'")
echo "skip consumed the dependency refresh (no refresh loop): $([ "$t1" = "$t2" ] && [ "$s" = "WaitingForDependencies" ] && echo yes || echo "no ($t1 -> $t2, $s)")"

# A real source change must still flow through: the next dependency refresh triggers a rebuild.
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (2)"
$CLICKHOUSE_CLIENT -q "SYSTEM REFRESH VIEW src_mv"
for _ in {1..120}
do
    n3=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM dep_mv")
    [ "$n3" -ge 2 ] && break
    sleep 0.5
done
echo "changed source triggers rebuild through dependency: $([ "$n3" -ge 2 ] && echo yes || echo "no ($n3)")"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE dep_mv SYNC;
    DROP TABLE src_mv SYNC;
    DROP TABLE src SYNC;
"
