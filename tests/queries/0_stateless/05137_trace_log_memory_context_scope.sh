#!/usr/bin/env bash
# Tags: no-fasttest, no-tsan, no-asan, no-msan, no-ubsan, no-debug
# no-fasttest: the trace collector is not enabled there.
# Sanitizer and debug builds allocate very differently, which makes the memory profiler steps unreliable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Writing a part of an `INSERT` runs under an accounting scope group
# (`ThreadGroup::createForScope`), whose memory tracker is at `VariableContext::Scope`.
# Check that the memory profiler traces it back with `memory_context = 'Scope'`,
# i.e. the enum value is present in `system.trace_log` and can be selected back.

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_scope_trace (k UInt64, v String)
ENGINE = MergeTree ORDER BY k
"

query_id="${CLICKHOUSE_DATABASE}_scope_trace_$RANDOM"

${CLICKHOUSE_CLIENT} --query_id "$query_id" --memory_profiler_step 1000000 --max_untracked_memory 1000000 --max_insert_threads 1 --query "
INSERT INTO t_scope_trace
SELECT number, repeat('a', 100) FROM numbers(3000000)
"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS trace_log"

# The insert allocates far more than one profiler step while writing the part,
# so at least one `Memory` trace must come from the scope tracker.
${CLICKHOUSE_CLIENT} --query "
SELECT count() > 0
FROM system.trace_log
WHERE event_date >= yesterday()
  AND query_id = '$query_id'
  AND trace_type = 'Memory'
  AND memory_context = 'Scope'
"
