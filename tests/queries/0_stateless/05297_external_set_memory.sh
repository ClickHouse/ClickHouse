#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-memory.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Runs the query in a fresh process with exact memory tracking and prints its result, how many sets spilled
# and why the first one did, or the error that stopped it.
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
        reason=$(grep -o -m 1 -E 'Switching the set of IN to external mode [^:]*: [^(]+' "${LOCAL_DIR}/${name}.log" \
            | sed -E 's/^Switching the set of IN to external mode [^:]*: //; s/ +$//' || true)
        echo "${name} ${result}${reason:+ ${reason}}"
    else
        # The message of the error can nest the message of the same error, so only its first code is printed.
        echo "${name}" "$(awk '/DB::Exception/ && match($0, /\([A-Z_]+\)/) { print substr($0, RSTART, RLENGTH); exit }' \
            "${LOCAL_DIR}/${name}.log")"
    fi
}

# The table of 524,288 16-byte keys takes 16 MiB, and the next chunk resizes it to 64 MiB: over the limit of
# the query in memory. On disk, the table and the memory to write it stay below the threshold, but the resize
# would not, so the projected growth of the table spills the set before the resize.
QUERY="SELECT count() FROM numbers(10) WHERE toUInt128(number) IN (SELECT toUInt128(number) FROM numbers(589824))"
memory_case query_limit_memory "${QUERY}" --max_memory_usage 64M --max_bytes_before_external_set 0
memory_case query_limit_disk "${QUERY}" --max_memory_usage 64M --max_bytes_before_external_set 48M

# The ratio applies to the memory left under the limit of the user.
memory_case user_ratio "${QUERY}" --max_memory_usage 0 --max_memory_usage_for_user 64M \
    --max_bytes_ratio_before_external_set 0.75

# 64 keys of 1 MiB fit the initial capacity of the table, but their arena grows past the limit of the user,
# which equals the threshold, so the projected growth must count the arena.
for key_type in String 'FixedString(1048592)'; do
    QUERY="SELECT count() FROM numbers(64) WHERE CAST(concat(toString(number), repeat('xxxxxxxx', 131072)), '${key_type}')
        IN (SELECT CAST(concat(toString(number), repeat('xxxxxxxx', 131072)), '${key_type}') FROM numbers(64))"
    memory_case "arena_${key_type%%(*}" "${QUERY}" --max_block_size 2 --max_memory_usage 0 --max_memory_usage_for_user 100M \
        --max_bytes_before_external_set 100M --allow_suspicious_fixed_string_types 1
done

