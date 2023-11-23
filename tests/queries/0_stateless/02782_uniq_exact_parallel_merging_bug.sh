#!/usr/bin/env bash
# Tags: long, no-random-settings, no-tsan, no-asan, no-ubsan, no-msan, no-parallel

# shellcheck disable=SC2154

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


clickhouse-client -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t(s String)
    ENGINE = MergeTree
    ORDER BY tuple();
"

clickhouse-client -q "insert into ${CLICKHOUSE_DATABASE}.t select number%10==0 ? toString(number) : '' from numbers_mt(1e7)"

clickhouse-benchmark -q "select count(distinct s) from ${CLICKHOUSE_DATABASE}.t settings max_memory_usage = '50Mi'" --ignore-error -c 16 -i 1000 2>/dev/null
