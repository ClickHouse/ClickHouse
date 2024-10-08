#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_SCHEMA_FILES}"
SOURCE_SCHEMA_FILE="${CURDIR}/format_schemas/03234_proto_complex_nested_repeated_noexception.proto"
TARGET_SCHEMA_FILE="${CLICKHOUSE_SCHEMA_FILES}/03234_proto_complex_nested_repeated_noexception.proto"
cp "${SOURCE_SCHEMA_FILE}" "${TARGET_SCHEMA_FILE}"

cat <<'EOF' | $CLICKHOUSE_CLIENT -mn

DROP TABLE IF EXISTS exception_counter ;
CREATE TABLE exception_counter (`val` UInt32) ENGINE = Memory ;
INSERT INTO exception_counter SELECT sum(value) FROM system.errors WHERE name = 'PROTOBUF_FIELD_NOT_REPEATED' ;

DROP TABLE IF EXISTS table_file ;
CREATE TABLE table_file (
    `i`     UInt32,
    `j.k`   Array(UInt32),
    `j.l`   Array(UInt32),
    `m`     UInt32,
    `n`     Array(UInt32),
    `o`     Nested(
                `key`   UInt32,
                `value` Tuple(
                    `p` Nested(
                        `q` UInt32,
                        `r` UInt32
                    )
                )
            )
) ENGINE File(Protobuf) SETTINGS format_schema = '03234_proto_complex_nested_repeated_noexception.proto:A' ;

INSERT INTO table_file VALUES
( 1001, [2101, 2102], [2201, 2202], 3001, [4001, 4002, 4003, 4004], [5001,5002] , [ ([(5111,5121)]), ([(5112,5122),(5113,5123)]) ] ),
( 6001, [7101], [7201], 8001, [], [9001] , [ ([(10111,10121)]) ] ) ;

INSERT INTO exception_counter SELECT sum(value) FROM system.errors WHERE name = 'PROTOBUF_FIELD_NOT_REPEATED' ;
SELECT min(val) == max(val) as error_not_raised FROM exception_counter FORMAT JSONEachRow ;

SELECT * FROM table_file FORMAT JSONEachRow ;

DROP TABLE exception_counter ;
DROP TABLE table_file ;

EOF

rm -f "${TARGET_SCHEMA_FILE}"
