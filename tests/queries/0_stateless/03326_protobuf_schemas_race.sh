#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --optimize_trivial_insert_select=0"

mkdir -p "${CLICKHOUSE_SCHEMA_FILES}"
mkdir -p "${CLICKHOUSE_SCHEMA_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
SOURCE_SCHEMA_FILE="${CURDIR}/format_schemas/03234_proto_simple_nested_repeated_noexception.proto"
TARGET_SCHEMA_FILE="${CLICKHOUSE_SCHEMA_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/03234_proto_simple_nested_repeated_noexception.proto"
cp "${SOURCE_SCHEMA_FILE}" "${TARGET_SCHEMA_FILE}"

echo "DROP TABLE IF EXISTS table_file;
CREATE TABLE table_file (
    u     UInt32,
    \`v.w\`   Array(UInt32),
    \`v.x\`   Array(UInt32),
    \`v.y\`   Array(Array(UInt32)),
    \`v.z\`   Array(Array(UInt32))
) ENGINE File(Protobuf) SETTINGS format_schema = '$CLICKHOUSE_TEST_UNIQUE_NAME/03234_proto_simple_nested_repeated_noexception.proto:M';
INSERT INTO table_file SELECT * FROM generateRandom() limit 1000000;
DROP TABLE table_file;" | $CLICKHOUSE_CLIENT -m &
INSERT_PID=$!

# Only the in-memory Protobuf cache takes part in the race: the schema of this test is a plain file
# in its own directory, not an entry of the on-disk `__cache__` directory. A bare `SYSTEM CLEAR
# FORMAT SCHEMA CACHE` would also delete the `__cache__` files of every test running in parallel
# (`format_schema_source = 'string'` / `'query'`), which then fail with `File not found`.
#
# Keep clearing the cache for as long as the INSERT is running, sending a whole batch of queries
# through a single client connection. Starting a separate client for every query took more than
# the time limit of the test in a sanitizer build - the connections, not the cache clearing, were
# the slow part.
CLEAR_BATCH=$(yes "SYSTEM CLEAR FORMAT SCHEMA CACHE FOR Protobuf;" | head -n 100)
while true
do
    $CLICKHOUSE_CLIENT -m -q "$CLEAR_BATCH"
    kill -0 "$INSERT_PID" 2>/dev/null || break
done

wait "$INSERT_PID"

rm -rf "${CLICKHOUSE_SCHEMA_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
