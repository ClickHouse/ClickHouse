#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-moved-keys.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# Each set gets 200,000 random keys in chunks of 8,192 rows. Once its table reaches the 1 MiB threshold, it
# spills before the next chunk and moves the keys of its table first. Every key representation does, except the
# fixed tables of `UInt8` and `UInt16` keys, which never grow that large.
QUERIES=$(cat <<'SQL'
WITH rhs AS (SELECT toUInt8(n % 256) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'key8', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, toUInt8(cityHash64(number) % 512) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT toUInt16(n % 65536) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'key16', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, toUInt16(cityHash64(number) % 65536) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT toUInt32(n) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'key32', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, toUInt32(cityHash64(number)) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT n FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'key64', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, cityHash64(number) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT (toUInt16(n % 65536), toUInt16(n % 7)) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'keys32', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, (toUInt16(cityHash64(number) % 65536), toUInt16(cityHash64(number) % 7)) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT (toUInt32(n), toUInt32(n % 7)) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'keys64', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, (toUInt32(cityHash64(number)), toUInt32(cityHash64(number) % 7)) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT toUInt128(n) * 3 FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'keys128', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, toUInt128(cityHash64(number)) * 3 IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT toUInt256(n) * 3 FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'keys256', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, toUInt256(cityHash64(number)) * 3 IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT toString(n) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'key_string', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, toString(cityHash64(number)) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT toFixedString(toString(n % 100000000), 40) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'key_fixed_string', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, toFixedString(toString(cityHash64(number) % 100000000), 40) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT (toString(n), n % 7) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'hashed', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, (toString(cityHash64(number)), cityHash64(number) % 7) IN rhs AS found FROM numbers(180000, 40000));
WITH rhs AS (SELECT if(n % 7 = 0, NULL, n) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'nullable_keys128', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, if(cityHash64(number) % 7 = 0, NULL, cityHash64(number)) IN rhs AS found FROM numbers(180000, 40000))
SETTINGS transform_null_in = 1;
WITH rhs AS (SELECT (if(n % 7 = 0, NULL, n), n % 3, n % 5) FROM (SELECT cityHash64(number) AS n FROM numbers(200000)))
SELECT 'nullable_keys256', sum(found), sum(cityHash64(number) * found)
FROM (SELECT number, (if(cityHash64(number) % 7 = 0, NULL, cityHash64(number)), cityHash64(number) % 3, cityHash64(number) % 5) IN rhs AS found FROM numbers(180000, 40000))
SETTINGS transform_null_in = 1;
SQL
)

# For each query by its label, how many sets spilled.
REPORT=$(cat <<'SQL'
SYSTEM FLUSH LOGS query_log;
SELECT 'report', extract(query, 'SELECT \'([^\']+)\'') AS label, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND label != ''
ORDER BY event_time_microseconds;
SQL
)

for threshold in 0 1048576; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${threshold}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 \
        --max_bytes_before_external_set "${threshold}" --max_block_size 8192 --send_logs_level trace --multiquery \
        <<< "${QUERIES}
${REPORT}" > "${LOCAL_DIR}/${threshold}.out" 2> "${LOCAL_DIR}/${threshold}.log"
done
diff -u <(grep -v '^report' "${LOCAL_DIR}/0.out") <(grep -v '^report' "${LOCAL_DIR}/1048576.out")
grep -v '^report' "${LOCAL_DIR}/1048576.out"
grep '^report' "${LOCAL_DIR}/1048576.out"

# Every spilled set moved keys from memory, as the trace log reports; without a threshold, none spills.
grep -o -E 'Switching the set of IN to external mode [^:]*: [^(]*\(keys in memory: [0-9]+' "${LOCAL_DIR}/1048576.log" \
    | grep -o -E '[0-9]+$' | awk '{ total += 1; moved += ($1 > 0) } END { print total, moved }'
grep -c 'Switching the set of IN to external mode' "${LOCAL_DIR}/0.log" || true
