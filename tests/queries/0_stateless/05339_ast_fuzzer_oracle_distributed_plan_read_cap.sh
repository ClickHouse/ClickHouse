#!/usr/bin/env bash
# Tags: no-fasttest, long, no-msan
# no-fasttest: the AST fuzzer and distributed plan settings are EXPERIMENTAL-tier.
# long: each sub-query of the oracle suite may take up to 10 s, so one heavy fuzzer mutation can exceed the flaky check's 180 s limit.
# no-msan: 48 fuzzed queries, each checked by the oracle suite over 900000 rows, take minutes under MSan and can exceed the 600 s
# timeout with `meta in keeper` and `s3 storage`. The mismatch it checks is a logical one, MSan does not help to find it.
# Random settings limits: optimize_trivial_count_query=(1, None)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# More rows than the fuzzed query's read cap (100000) in every reader bucket, so a stage task that sees it stops early.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t SELECT number, 'hit' FROM numbers(900000);
"

# A query without aggregates reaches NoREC (`executeScalar`); a full-table `max(id)` gives DQP (`executeWithSettings`) a read past the cap.
rounds=$(for _ in $(seq 1 16); do echo "SELECT count() FROM t WHERE val = 'hit'; SELECT id FROM t WHERE id >= 899500; SELECT max(id) FROM t;"; done)

mismatches=$($CLICKHOUSE_CLIENT --ignore-error --query "
    SET send_logs_level = 'none';
    SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 3,
        enable_parallel_replicas = 0, max_rows_to_group_by = 0;
    SET ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;
    $rounds
" 2>&1 >/dev/null | grep -c 'oracle mismatch')
echo "oracle mismatches: $mismatches"

# A slow build (ASan on S3 storage) can run every TLP sub-query past the oracle's own 10 s limit; such a sub-query still ran
# under the distributed plan, so it counts too.
$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'TLP Aggregate checked', count() > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND (type = 'QueryFinish' OR (type = 'ExceptionWhileProcessing' AND exception_code = 159))
        AND is_internal AND query ILIKE 'SELECT countMerge(%' AND ProfileEvents['DistributedPlanLocalExecution'] > 0;
    SELECT 'NoREC checked', count() > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryStart'
        AND is_internal AND query ILIKE 'SELECT countIf(%';
"
