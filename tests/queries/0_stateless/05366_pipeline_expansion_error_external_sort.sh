#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a failpoint.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every expansion of the pipeline fails on the failpoint, and the external sort of each stream expands it. Every query
# must fail with `MEMORY_LIMIT_EXCEEDED`, also when another thread is expanding at the same time. That is rare, so the
# queries run in batches for a while.
BATCH=30
QUERY="SELECT number FROM numbers_mt(600000) ORDER BY number DESC FORMAT Null SETTINGS max_threads = 16, max_block_size = 65409, max_bytes_before_external_sort = 200000, max_bytes_ratio_before_external_sort = 0;"
QUERIES="SYSTEM ENABLE FAILPOINT executing_graph_add_node_fail; $(for _ in $(seq 1 $BATCH); do echo "$QUERY"; done) SELECT 'alive';"

SECONDS=0
while [[ $SECONDS -lt 15 ]]; do
    output=$(${CLICKHOUSE_LOCAL} --ignore-error --query "$QUERIES" 2>&1)
    result=$(echo "$output" | grep -oE 'Code: 241|^alive$' | LC_ALL=C sort | uniq -c | awk '{c = $1; $1 = ""; print substr($0, 2), c}')
    if [[ "$result" != "Code: 241 $BATCH"$'\n'"alive 1" ]]; then
        echo "$result"
        echo "$output" | grep -vE '^Received exception:$|^Code: 241\. |^\(query: ' | head -n 30
        break
    fi
done
echo "done"
