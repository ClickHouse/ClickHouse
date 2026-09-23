#!/usr/bin/env bash
# Tags: long, no-flaky-check, no-random-detach, no-replicated-database, no-fasttest
# no-random-detach: test uses DETACH/ATTACH itself
# long: a comprehensive regression suite whose cumulative time across flaky-check reruns exceeds the
#       flaky-check budget, though each run is quick.
# no-flaky-check: the flaky check reruns a test 50 times; a single run of this suite already takes tens
#       of seconds on a sanitizer build, so the reruns hit the per-test timeout. The suite is
#       deterministic (it drives every `DETACH`/`ATTACH` itself and is `no-random-detach`), so there is
#       nothing for the flaky check to shake out here.
# no-fasttest: part of the `long` reattach suite; the fast test runs with `--no-long` and never runs it.

# The internal `DETACH`/`ATTACH` pair must not inherit the settings of the query that triggered it. Those
# settings describe how that statement reads its data, and one of them making the `ATTACH` back fail leaves
# the table detached: the recovery `ATTACH` inherits the same setting and fails the same way.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_internal_settings"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_internal_settings (a UInt64) ENGINE = MergeTree ORDER BY a"

# `distributed_cache_client_id` is the setting that exposed this: a query reading through the distributed
# cache with a deliberately invalid id had the internal `ATTACH` inherit it and be rejected while the cache
# client was constructed. It is inert for a plain `MergeTree` read, so here it only has to travel (or not).
check_if_detached "SELECT count() FROM t_reattach_internal_settings SETTINGS distributed_cache_client_id = 'reattach_marker'" "t_reattach_internal_settings"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

# Both internal statements must be there (so the check below cannot pass by finding nothing), and neither
# may carry the caller's setting.
${CLICKHOUSE_CLIENT} -q "
    SELECT
        countIf(query = 'DETACH TABLE ${CLICKHOUSE_DATABASE}.t_reattach_internal_settings SYNC') AS detach_logged,
        countIf(query = 'ATTACH TABLE ${CLICKHOUSE_DATABASE}.t_reattach_internal_settings') AS attach_logged,
        countIf(mapContains(Settings, 'distributed_cache_client_id')) AS inherited_caller_setting
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND type = 'QueryStart'
      AND query IN (
            'DETACH TABLE ${CLICKHOUSE_DATABASE}.t_reattach_internal_settings SYNC',
            'ATTACH TABLE ${CLICKHOUSE_DATABASE}.t_reattach_internal_settings')"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_internal_settings"
