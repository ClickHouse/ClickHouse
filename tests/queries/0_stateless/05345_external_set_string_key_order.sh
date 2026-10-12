#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-string-key-order.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# The disk key of a string keeps the order of its first 4 bytes, and a hash tells apart the strings that share
# them. With a threshold of 1 byte, each set spills before its first chunk and, in the `break` mode, keeps the
# merged blocks of 50 keys up to the 100th: its 100 smallest disk keys. Each query counts the probed strings
# that the set finds and those of them among the 100 smallest strings, which arrive scattered. Strings that
# differ in their first 4 bytes keep their order, as `String` and as `FixedString`, so the set keeps the 100
# smallest.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" \
    --max_bytes_before_external_set 1 --max_rows_in_set 100 --set_overflow_mode break --max_block_size 50 <<'SQL'
SELECT 'String', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, concat(leftPad(toString(i), 4, '0'), 'abcdefgh')
    IN (SELECT concat(leftPad(toString(n), 4, '0'), 'abcdefgh') FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'String';
SELECT 'FixedString(40)', countIf(found), countIf(found AND i < 100)
FROM (SELECT number AS i, toFixedString(leftPad(toString(i), 4, '0'), 40)
    IN (SELECT toFixedString(leftPad(toString(n), 4, '0'), 40) FROM (SELECT number * 7919 % 1000 AS n FROM numbers(1000))) AS found FROM numbers(1000))
SETTINGS log_comment = 'FixedString(40)';

-- Each pair of strings is equal in the first 4 bytes, padded with zero bytes, so only the hashes tell the
-- pair apart: the empty string and a zero byte, a string with and without a trailing zero byte, and strings
-- that differ after their first 4 bytes. With a threshold of 1 MiB, the set takes chunks of 8192 strings
-- until its table reaches the threshold. The members come first, so the set moves them from its table at the
-- spill, where the empty string has no bytes. The set finds its members and none of the other probes.
CREATE TABLE probes (s String, member UInt8) ENGINE = Memory;
INSERT INTO probes VALUES ('', 1), ('\0', 0), ('a', 1), ('a\0', 0), ('abcde', 1), ('abcdf', 0);
SELECT 'edge values', countIf(found AND member), countIf(found AND NOT member)
FROM (SELECT member, s IN (
        SELECT s FROM (SELECT s, 0 AS filler FROM probes WHERE member
            UNION ALL SELECT concat('filler', toString(number)), 1 FROM numbers(100000)) ORDER BY filler)
    AS found FROM probes)
SETTINGS max_rows_in_set = 0, max_bytes_before_external_set = 1048576, max_block_size = 8192,
    log_comment = 'edge values';

-- Every set spilled.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsSpilledToDisk']
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment != '' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;
SQL
