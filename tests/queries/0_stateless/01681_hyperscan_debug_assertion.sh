#!/usr/bin/env bash
# Tags: no-debug, no-fasttest
# Tag no-fasttest: Hyperscan

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We throw our own exception from operator new.
# In previous versions of Hyperscan it triggered debug assertion as it only expected std::bad_alloc.

M=1000000

i=0 retries=300
while [[ $i -lt $retries ]]; do
    $CLICKHOUSE_CLIENT --allow_hyperscan 1 --max_memory_usage $M --format Null --query "
        SELECT [1, 2, 3, 11] = arraySort(multiMatchAllIndices('фабрикант', ['', 'рикан', 'а', 'f[a${RANDOM}e]b[ei]rl', 'ф[иа${RANDOM}эе]б[еэи][рпл]', 'афиукд', 'a[f${RANDOM}t],th', '^ф[аие${RANDOM}э]?б?[еэи]?$', 'бе${RANDOM}рлик', 'fa${RANDOM}b', 'фа[беьв]+е?[рл${RANDOM}ко]']))
    " 2>&1 | grep -q 'Memory limit' || break;

    M=$((M + 100000))
    ((++i))
done

echo 'Ok'
