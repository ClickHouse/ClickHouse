#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME-schema
FILE=$CLICKHOUSE_TEST_UNIQUE_NAME

$CLICKHOUSE_LOCAL -q "select 42 as \`a.b.c\`, 43 as \`a.b.d.e\`, 44 as \`a.b.f\`, 45 as \`a.g\`, 46 as \`h.k\` format Protobuf settings output_format_schema='$SCHEMA_FILE.proto'" > $FILE.pb
tail -n +2 $SCHEMA_FILE.proto
$CLICKHOUSE_LOCAL -q "select * from file('$FILE.pb') settings format_schema='$SCHEMA_FILE:Message'"

$CLICKHOUSE_LOCAL -q "select 42 as a_b_c, 43 as a_b_d_e, 44 as a_b_f, 45 as a_g, 46 as h_k format CapnProto settings output_format_schema='$SCHEMA_FILE.capnp'" > $FILE.capnp
tail -n +2 $SCHEMA_FILE.capnp
$CLICKHOUSE_LOCAL -q "select * from file('$FILE.capnp') settings format_schema='$SCHEMA_FILE:Message'"

rm $SCHEMA_FILE*
rm $FILE.*

