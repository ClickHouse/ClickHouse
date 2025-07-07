#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# create or replace resource and workload
$CLICKHOUSE_CLIENT -nm -q "
    CREATE OR REPLACE RESOURCE worker_cpu (WORKER THREAD);

    CREATE OR REPLACE RESOURCE master_cpu (MASTER THREAD);

    CREATE OR REPLACE WORKLOAD all
    SETTINGS max_concurrent_threads = 16 FOR worker_cpu,
            max_concurrent_threads = 32 FOR master_cpu;
"

# run benchmark with max_threads and workload
$CLICKHOUSE_BENCHMARK --randomize --timelimit 10 --continue_on_errors --concurrency 10 >& /dev/null <<EOL
SELECT sum(number) FROM numbers(100000000) settings max_threads=1, workload = 'all';
EOL

# server is alive
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null"

# clean resource and workload
$CLICKHOUSE_CLIENT -nm -q "
    DROP WORKLOAD all;
    DROP RESOURCE worker_cpu;
    DROP RESOURCE master_cpu;
"
