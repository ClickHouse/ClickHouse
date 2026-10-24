#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-after-build.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Runs queries in a fresh process with a 48 MiB spill threshold and prints their results, the number of sets
# that spilled, and how many of them switched to disk while they were built and while they were used.
#
# Each set but the small one holds 140,000 16-byte keys in 16 MiB, so it stays in memory while it is built.
# The query then collects 4 KiB strings of the rows that pass `IN`, 2 MiB per chunk of 1,024 rows, so query
# memory exceeds the threshold while the set is used, by the 17th of 20 chunks in any order of the threads.
run()
{
    local name="$1"
    local query="$2"
    local out="${LOCAL_DIR}/${name}.out"
    local log="${LOCAL_DIR}/${name}.log"
    if ! ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --max_bytes_before_external_set 50331648 \
        --max_untracked_memory 0 --max_block_size 1024 --send_logs_level trace --multiquery \
        > "${out}" 2> "${log}" <<SQL
${query};
SELECT 'spilled', sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk';
SQL
    then
        grep -v '<Trace>\|<Debug>\|<Information>' "${log}" >&2
        return 1
    fi

    local while_built while_used
    while_built=$(grep -c 'Switching the set of IN to external mode while it is built' "${log}" || true)
    while_used=$(grep -c 'Switching the set of IN to external mode while it is used' "${log}" || true)
    printf '%s\t%s\twhile built %d, while used %d\n' "${name}" "$(paste -s "${out}")" "${while_built}" "${while_used}"
}

# A numeric set, looked up by one thread, which looks up the chunks after the switch on disk.
run numbers "SELECT count(), sum(number), length(groupArray(s))
    FROM (SELECT number, repeat('x', 4096) AS s FROM numbers(20480))
    WHERE toUInt128(number) NOT IN (SELECT toUInt128(number * 2) FROM numbers(140000)) SETTINGS max_threads = 1"

# Four threads look up the set. One of them spills it while the others keep reading the table.
run numbers_threads "SELECT count(), sum(number), length(groupArray(s))
    FROM (SELECT number, repeat('x', 4096) AS s FROM numbers_mt(20480))
    WHERE toUInt128(number) NOT IN (SELECT toUInt128(number * 2) FROM numbers(140000)) SETTINGS max_threads = 4"

# The set of a primary key condition is built with explicit elements for index analysis, and it keeps them
# when it spills while it is used. Its keys below 20000 select 20 of the 30 granules, and the others lie above
# the table.
run explicit_elements "CREATE TABLE t (k UInt128) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
    INSERT INTO t SELECT number FROM numbers(30000);
    SELECT count(), sum(k), length(groupArray(repeat('x', 4096))) FROM t
    WHERE k IN (SELECT toUInt128(if(number < 10000, number * 2, 10000000 + number)) FROM numbers(140000))
    SETTINGS max_threads = 1;
    SELECT 'selected marks', sum(value) FROM system.events WHERE event = 'SelectedMarks'"

# A set smaller than 16 MiB stays in memory, however much memory the rest of the query takes.
run small "SELECT count(), sum(number), length(groupArray(s))
    FROM (SELECT number, repeat('x', 4096) AS s FROM numbers(20480))
    WHERE number NOT IN (SELECT number * 2 FROM numbers(1000)) SETTINGS max_threads = 1"
