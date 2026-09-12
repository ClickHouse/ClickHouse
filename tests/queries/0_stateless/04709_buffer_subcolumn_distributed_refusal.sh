#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# StorageBuffer::read throws an unrelated NOT_IMPLEMENTED on this same engine combination, so the
# error code alone does not pin down which refusal fired. A .sql test cannot assert message text.

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE dst (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dst VALUES ('[1,2,3,4,5,6]');
CREATE TABLE dist AS dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), dst);
CREATE TABLE buf (arr Array(UInt8))
    ENGINE = Buffer(currentDatabase(), dist, 1, 100, 100, 1000, 10000, 1000000, 10000000);
"

MSG=$(${CLICKHOUSE_CLIENT} --send_logs_level=none \
    -q "SELECT arr.size0 FROM buf SETTINGS optimize_functions_to_subcolumns = 0" 2>&1 \
    | grep -m1 -F 'DB::Exception:')

# Match against the exception message only, not the query text the client echoes after it.
echo "$MSG" | grep -o -m1 -F 'NOT_IMPLEMENTED'
echo "$MSG" | grep -o -m1 -F 'arr.size0'

${CLICKHOUSE_CLIENT} -q "DROP TABLE buf; DROP TABLE dist; DROP TABLE dst;"
