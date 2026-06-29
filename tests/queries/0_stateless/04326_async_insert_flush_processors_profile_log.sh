#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Tests that processors of an asynchronous insert flush get per-processor profiling,
# i.e. elapsed_us in system.processors_profile_log is populated (not stuck at zero).
# The flush builds and runs its pipeline directly, so it must propagate the process list
# element to the pipeline for the executor to enable profiling.

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_flush_ppl"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_flush_ppl (number UInt64) ENGINE = MergeTree ORDER BY number"

# Async inserts only apply when the data is inlined in the request (not for INSERT ... SELECT),
# so feed the data through stdin.
${CLICKHOUSE_CLIENT} \
    --async_insert 1 --wait_for_async_insert 1 --log_processors_profiles 1 \
    -q "INSERT INTO t_async_flush_ppl FORMAT TSV" < <(seq 0 99999)

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log, processors_profile_log"

# At least one processor of the flush pipeline must report non-zero elapsed time.
${CLICKHOUSE_CLIENT} -q "
    SELECT sum(elapsed_us) > 0
    FROM system.processors_profile_log
    WHERE event_date >= yesterday()
      AND query_id IN (
          SELECT query_id
          FROM system.query_log
          WHERE event_date >= yesterday()
            -- The flush runs as a background query whose current_database is always 'default',
            -- so match on the databases array instead (also what the style check accepts).
            AND has(databases, currentDatabase())
            AND query_kind = 'AsyncInsertFlush'
            AND type = 'QueryFinish'
            AND has(tables, currentDatabase() || '.t_async_flush_ppl')
      )"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_async_flush_ppl"
