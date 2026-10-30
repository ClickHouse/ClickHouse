#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-duplicate-probes.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# A set on disk looks up each distinct key of a chunk once and gives its answer to every row with the key: the
# rows of a run of equal keys take the answer of the first one, and the other probes of equal keys meet when
# they are sorted. With a threshold of 1 byte, each set spills before its first chunk. Each query counts the
# rows that the set finds and sums a value of theirs, so a wrong answer for any row shows.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --max_bytes_before_external_set 1 <<'SQL'
-- 100 keys repeat 1000 times each, scattered over the rows. The set holds the 50 even ones.
SELECT 'scattered', countIf(found), sumIf(number % 100, found)
FROM (SELECT number, cityHash64(number % 100) IN (SELECT cityHash64(number * 2) FROM numbers(50)) AS found FROM numbers(100000))
SETTINGS log_comment = 'scattered';
-- 10000 keys come in runs of 7 rows. The set holds the 2000 multiples of 3.
SELECT 'runs', countIf(found), sumIf(intDiv(number, 7), found)
FROM (SELECT number, cityHash64(intDiv(number, 7)) IN (SELECT cityHash64(number * 3) FROM numbers(2000)) AS found FROM numbers(70000))
SETTINGS log_comment = 'runs';
-- The keys from 0 to 2999 repeat 10 times each. The set holds the keys from 1000 to 1999, so the probes below
-- its first key and above its last key find nothing.
SELECT 'outside the set', countIf(found), sumIf(number % 3000, found)
FROM (SELECT number, number % 3000 IN (SELECT number + 1000 FROM numbers(1000)) AS found FROM numbers(30000))
SETTINGS log_comment = 'outside the set';
SELECT 'strings', countIf(found), sumIf(number % 100, found)
FROM (SELECT number, toString(number % 100) IN (SELECT toString(number * 2) FROM numbers(50)) AS found FROM numbers(100000))
SETTINGS log_comment = 'strings';
SELECT 'not in', countIf(not_found), sumIf(number % 100, not_found)
FROM (SELECT number, cityHash64(number % 100) NOT IN (SELECT cityHash64(number * 2) FROM numbers(50)) AS not_found FROM numbers(100000))
SETTINGS log_comment = 'not in';
-- A `NULL` of a nullable column holds the default value 0 underneath. The rows alternate between `NULL` and
-- 0, so they form one run of key 0, which a `NULL` starts. The set holds 0 and finds only the rows that are
-- not `NULL`.
CREATE TABLE nullable_run (x Nullable(UInt64)) ENGINE = Memory;
INSERT INTO nullable_run SELECT if(number % 2 = 0, NULL, 0) FROM numbers(1000);
SELECT 'nullable', countIf(x IN (SELECT number FROM numbers(10))), count() FROM nullable_run
SETTINGS log_comment = 'nullable';

-- Every set spilled.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL
