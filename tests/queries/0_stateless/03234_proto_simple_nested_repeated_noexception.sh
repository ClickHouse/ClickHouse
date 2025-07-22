#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_SCHEMA_FILES}"
SOURCE_SCHEMA_FILE="${CURDIR}/format_schemas/03234_proto_simple_nested_repeated_noexception.proto"
TARGET_SCHEMA_FILE="${CLICKHOUSE_SCHEMA_FILES}/03234_proto_simple_nested_repeated_noexception.proto"
cp "${SOURCE_SCHEMA_FILE}" "${TARGET_SCHEMA_FILE}"

cat <<'EOF' | $CLICKHOUSE_CLIENT -mn

DROP TABLE IF EXISTS exception_counter ;
CREATE TABLE exception_counter (`val` UInt32) ENGINE = Memory ;
INSERT INTO exception_counter SELECT sum(value) FROM system.errors WHERE name = 'PROTOBUF_FIELD_NOT_REPEATED' ;

DROP TABLE IF EXISTS table_file ;
CREATE TABLE table_file (
    `u`     UInt32,
    `v.w`   Array(UInt32),
    `v.x`   Array(UInt32),
    `v.y`   Array(Array(UInt32)),
    `v.z`   Array(Array(UInt32))
) ENGINE File(Protobuf) SETTINGS format_schema = '03234_proto_simple_nested_repeated_noexception.proto:M' ;

INSERT INTO table_file VALUES
( 1001, [], [], [], []),
( 2002, [2102], [2202], [[2302]], [[2402, 2403]]),
( 3003, [3103, 3104], [3203, 3204], [[3303], [3304]], [[3403, 3404], [3405, 3406]]),
( 4004, [4104, 4105, 4106], [4204, 4205, 4206], [[4304], [4305], [4306]], [[4304, 4305], [4306, 4307], [4308, 4309]]);


INSERT INTO exception_counter SELECT sum(value) FROM system.errors WHERE name = 'PROTOBUF_FIELD_NOT_REPEATED' ;
SELECT min(val) == max(val) as error_not_raised FROM exception_counter FORMAT JSONEachRow ;

SELECT * FROM table_file FORMAT JSONEachRow ;

DROP TABLE exception_counter ;
DROP TABLE table_file ;

EOF

rm -f "${TARGET_SCHEMA_FILE}"
