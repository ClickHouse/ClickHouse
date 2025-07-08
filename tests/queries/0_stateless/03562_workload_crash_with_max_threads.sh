#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# create or replace resource and workload
$CLICKHOUSE_CLIENT -nm -q "
    CREATE OR REPLACE RESOURCE 03562_worker_cpu (WORKER THREAD);

    CREATE OR REPLACE RESOURCE 03562_master_cpu (MASTER THREAD);

    CREATE OR REPLACE WORKLOAD 03562_wl
    SETTINGS max_concurrent_threads = 16 FOR 03562_worker_cpu,
            max_concurrent_threads = 32 FOR 03562_master_cpu;
"

# run benchmark with max_threads and workload
$CLICKHOUSE_BENCHMARK --randomize --timelimit 10 --continue_on_errors --concurrency 10 >& /dev/null <<EOL
SELECT sum(number) FROM numbers(100000000) settings max_threads = 1 , workload = '03562_wl';
EOL

# server is alive
$CLICKHOUSE_CLIENT -q "SELECT 1"

# clean resource and workload
$CLICKHOUSE_CLIENT -nm -q "
    DROP WORKLOAD 03562_wl;
    DROP RESOURCE 03562_worker_cpu;
    DROP RESOURCE 03562_master_cpu;
"
