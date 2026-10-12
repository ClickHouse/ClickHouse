#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-tsan
# clickhouse-local writes the fatal signal report to stderr and to --client_logs_file when stderr is not a terminal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP:?}/04916_${CLICKHOUSE_DATABASE:?}"
rm -rf "${dir:?}"
mkdir -p "$dir"
cd "$dir" || exit 1

# No core dump, CI collects them from the run directory.
ulimit -c 0

$CLICKHOUSE_LOCAL --client_logs_file=client.log --max_threads=1 \
    --query "SYSTEM STOP THREAD FUZZER; SELECT 'ready'; SELECT sleep(1) FROM numbers(60) SETTINGS max_block_size = 1 FORMAT Null" \
    >stdout 2>stderr &
pid=$!

for _ in {1..600}; do
    grep -q ready stdout && break
    sleep 0.1
done
# Signal the process while it idles in sleep(), not while it finishes the previous query.
sleep 1

{ kill -ABRT "$pid"; wait "$pid"; } 2>/dev/null
echo "exit code $?"
echo "stderr $(grep -c 'Signal description: Aborted' stderr)"
echo "client log $(grep -c 'Signal description: Aborted' client.log)"

cd .. && rm -rf "${dir:?}"
