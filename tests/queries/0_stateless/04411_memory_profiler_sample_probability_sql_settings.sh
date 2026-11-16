#!/usr/bin/env bash
# Tags: no-fasttest, no-flaky-check

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# memory_profiler_sample_probability in the SQL SETTINGS clause (not as a client/profile setting)
# reaches the query memory tracker only in ProcessList::insert, after the initiating thread already
# cached its sample config on attach. range(1000000) is an ~8MB array constant-folded on the
# initiating thread during analysis (after insert); the min_allocation_size filter keeps only that
# large allocation, so a MemorySample appears iff the initiating thread's cache was refreshed.

query_id="04411_${CLICKHOUSE_DATABASE}_${RANDOM}_$$"
${CLICKHOUSE_CLIENT} --query_id "$query_id" -q "SELECT ignore(range(1000000)) SETTINGS memory_profiler_sample_probability = 1, memory_profiler_sample_min_allocation_size = 64000 FORMAT Null"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS trace_log"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.trace_log WHERE query_id = '$query_id' AND trace_type = 'MemorySample'"
