#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AST fuzzer and distributed plan settings are EXPERIMENTAL-tier.
# Random settings limits: optimize_trivial_count_query=(1, None)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# More rows than the fuzzed query's read cap (100000) in every reader bucket, so a stage task that sees it stops early.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t SELECT number, 'hit' FROM numbers(900000);
"

rounds=$(for _ in $(seq 1 8); do echo "SELECT count() FROM t WHERE val = 'hit';"; done)

mismatches=$($CLICKHOUSE_CLIENT --ignore-error --query "
    SET send_logs_level = 'none';
    SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 3,
        enable_parallel_replicas = 0, max_rows_to_group_by = 0;
    SET ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;
    $rounds
" 2>&1 >/dev/null | grep -c 'oracle mismatch')
echo "oracle mismatches: $mismatches"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'TLP Aggregate checked', count() > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND is_internal AND query ILIKE 'SELECT countMerge(%';
"
