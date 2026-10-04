#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-memory.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Runs the query in a fresh process with exact memory tracking and prints its result, how many sets spilled and
# why the first one did, or the error that stopped it.
memory_case()
{
    local name="$1"
    local query="$2"
    shift 2
    local out
    if out=$(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --max_threads 1 --max_untracked_memory 0 "$@" \
        --send_logs_level trace --multiquery 2> "${LOCAL_DIR}/${name}.log" <<SQL
${query};
SELECT sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk';
SQL
    ); then
        local result reason
        result=$(tr '\n' ' ' <<< "${out}" | sed -E 's/ +$//')
        reason=$(grep -o -m 1 -E 'Switching the set of IN to external mode: [^(]+' "${LOCAL_DIR}/${name}.log" \
            | sed -E 's/^Switching the set of IN to external mode: //; s/ +$//' || true)
        echo "${name} ${result}${reason:+ ${reason}}"
    else
        echo "${name}" "$(grep -o -m 1 -E '\([A-Z_]+\)' "${LOCAL_DIR}/${name}.log")"
    fi
}

# The table of 524,288 16-byte keys takes 16 MiB, and the next chunk resizes it to 64 MiB: over the limit of the
# query in memory, within it on disk.
QUERY="SELECT count() FROM numbers(10) WHERE toUInt128(number) IN (SELECT toUInt128(number) FROM numbers(589824))"
memory_case query_limit_memory "${QUERY}" --max_memory_usage 64M --max_bytes_before_external_set 0
memory_case query_limit_disk "${QUERY}" --max_memory_usage 64M --max_bytes_before_external_set 30M

# The ratio applies to the memory left under the limit of the user.
for ratio in 0 0.5; do
    memory_case "user_ratio_${ratio}" "${QUERY}" --max_memory_usage 0 --max_memory_usage_for_user 64M \
        --max_bytes_ratio_before_external_set "${ratio}"
done

# The table fits below the threshold, but the resize of the next chunk would exceed the limit of the query or
# of the user, so only the projected growth spills the set in time.
memory_case growth_query "${QUERY}" --max_block_size 65536 --max_memory_usage 64M --max_bytes_before_external_set 48M
memory_case growth_user "${QUERY}" --max_block_size 65536 --max_memory_usage 0 --max_memory_usage_for_user 64M \
    --max_bytes_before_external_set 48M

# 64 keys of 1 MiB fit the initial capacity of the table, but their arena grows past the limit of the user,
# which equals the threshold, so the projected growth must count the arena.
for key_type in String 'FixedString(1048592)'; do
    QUERY="SELECT count() FROM numbers(100) WHERE CAST(concat(toString(number), repeat('xxxxxxxx', 131072)), '${key_type}')
        IN (SELECT CAST(concat(toString(number), repeat('xxxxxxxx', 131072)), '${key_type}') FROM numbers(64))"
    for threshold in 0 100M; do
        memory_case "arena_${key_type%%(*}_${threshold}" "${QUERY}" --max_block_size 2 --max_memory_usage 0 \
            --max_memory_usage_for_user 100M --max_bytes_before_external_set "${threshold}" --allow_suspicious_fixed_string_types 1
    done
done

# The fixed tables of `UInt8` and `UInt16` keys never grow, so their sets stay in memory under a small limit.
for key_type in UInt8 UInt16; do
    memory_case "fixed_${key_type}" "SELECT count() FROM numbers(70000) WHERE to${key_type}(number) IN
        (SELECT to${key_type}(number) FROM numbers(1048576))" --max_block_size 65536 --max_memory_usage 0 \
        --max_memory_usage_for_user 16M --max_bytes_before_external_set 1G
done

# A set of hashed keys within the limit of the user stays in memory with the ratio too.
QUERY="SELECT count() FROM numbers(16384) WHERE [concat(toString(number), repeat('x', 4096))]
    IN (SELECT [concat(toString(number), repeat('x', 4096))] FROM numbers(16384))"
for ratio in 0 0.5; do
    memory_case "hashed_ratio_${ratio}" "${QUERY}" --max_block_size 2048 --max_memory_usage 0 --max_memory_usage_for_user 120M \
        --max_bytes_ratio_before_external_set "${ratio}"
done
