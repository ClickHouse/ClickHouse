#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: needs the protobuf library.
# no-parallel: uses SYSTEM FLUSH ASYNC INSERT QUEUE.

# Regression test: ProtobufListInputFormat::resetParser must rewind both the
# serializer envelope state and the underlying ProtobufReader's message-bounds
# tracking. Otherwise, when a single format instance parses two envelopes in
# a row (as happens with async-insert batching, Kafka, etc.), the second
# envelope hits `chassert(!current_message_level)` in ProtobufReader::startMessage
# in debug builds.
#
# This must go through the HTTP interface, not the native protocol:
# clickhouse-client over TCP pushes pre-parsed Blocks into the async-insert
# queue (Preprocessed kind, format replaced with Native -- see
# TCPHandler.cpp:1334), so the server-side ProtobufList parser never runs.
# The HTTP path keeps the data as a raw byte string (Parsed kind), which is
# what triggers the ProtobufListInputFormat reuse across entries inside
# AsynchronousInsertQueue::processEntriesWithParsing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eo pipefail

SCHEMA_NAME=04249_protobuflist_reset_parser
LOCAL_SCHEMA="$CUR_DIR/format_schemas/$SCHEMA_NAME:Message"
mkdir -p "$CLICKHOUSE_SCHEMA_FILES"
cp "$CUR_DIR/format_schemas/$SCHEMA_NAME.proto" \
   "$CLICKHOUSE_SCHEMA_FILES/$SCHEMA_NAME.proto"

SERVER_SCHEMA="$SCHEMA_NAME:Message"

BINARY_FILE=$(mktemp "$CUR_DIR/04249_protobuflist_reset_parser.XXXXXX.binary")
BAD_BINARY_FILE=$(mktemp "$CUR_DIR/04249_protobuflist_reset_parser_bad.XXXXXX.binary")
PARTIAL_FIELD_BINARY_FILE=$(mktemp "$CUR_DIR/04249_protobuflist_reset_parser_partial_field.XXXXXX.binary")
C1_ONLY_BINARY_FILE=$(mktemp "$CUR_DIR/04249_protobuflist_reset_parser_c1_only.XXXXXX.binary")
cleanup()
{
    rm -f "$BINARY_FILE"
    rm -f "$BAD_BINARY_FILE"
    rm -f "$PARTIAL_FIELD_BINARY_FILE"
    rm -f "$C1_ONLY_BINARY_FILE"
    rm -f "$CLICKHOUSE_SCHEMA_FILES/$SCHEMA_NAME.proto"
}
trap cleanup EXIT

# Build source data.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS t_04249_src;
DROP TABLE IF EXISTS t_04249_dst;
CREATE TABLE t_04249_src (c0 Int32) ENGINE = Memory;
CREATE TABLE t_04249_dst (c0 Int32, c1 Int32) ENGINE = Memory;
INSERT INTO t_04249_src VALUES (1), (2), (3);
EOF

# Build a single ProtobufList envelope through clickhouse-client stdout. Do
# not use `INTO OUTFILE` here: that makes the server create the temporary
# file, which can leave it unreadable for the following curl requests.
$CLICKHOUSE_CLIENT --query "
    SELECT * FROM t_04249_src ORDER BY c0
    FORMAT ProtobufList
    SETTINGS format_schema = '$LOCAL_SCHEMA'
" > "$BINARY_FILE"

# A one-byte input is a truncated ProtobufList envelope: `startMessage` reads
# the root length delimiter, then parsing fails before `endMessage` can clear
# the reader's message-bounds state. The next queued entry only works if
# `ProtobufListInputFormat::resetParser` calls `reader->resetState`.
printf '\x01' > "$BAD_BINARY_FILE"

# This envelope has one row where `c0` was already read, but `c1` is truncated.
# The next row contains only `c1`. It needs default insertion for absent `c0`,
# which only happens if `field_read` is cleared while resetting the parser.
printf '\x05\x0a\x03\x08\x05\x10' > "$PARTIAL_FIELD_BINARY_FILE"
printf '\x04\x0a\x02\x10\x07' > "$C1_ONLY_BINARY_FILE"

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&async_insert_busy_timeout_max_ms=300000&async_insert_busy_timeout_min_ms=300000&async_insert_use_adaptive_busy_timeout=0&format_schema=$SERVER_SCHEMA&query=INSERT+INTO+${CLICKHOUSE_DATABASE}.t_04249_dst+FORMAT+ProtobufList"

# Send one bad envelope followed by the same valid envelope as three separate
# async-insert HTTP requests with a long busy-timeout so they accumulate in the
# same batch. The batch processor reuses one ProtobufListInputFormat for all
# entries and calls resetParser between them.
${CLICKHOUSE_CURL} -sS "$url" --data-binary @"$BAD_BINARY_FILE"
${CLICKHOUSE_CURL} -sS "$url" --data-binary @"$PARTIAL_FIELD_BINARY_FILE"
${CLICKHOUSE_CURL} -sS "$url" --data-binary @"$C1_ONLY_BINARY_FILE"
${CLICKHOUSE_CURL} -sS "$url" --data-binary @"$BINARY_FILE"
${CLICKHOUSE_CURL} -sS "$url" --data-binary @"$BINARY_FILE"
${CLICKHOUSE_CURL} -sS "$url" --data-binary @"$BINARY_FILE"

# Flush, verify all nine rows landed (the parser would have asserted before
# the fix), and clean up in one final client invocation.
$CLICKHOUSE_CLIENT --multiquery <<EOF
SYSTEM FLUSH ASYNC INSERT QUEUE t_04249_dst;
SELECT count(), sum(c0), sum(c1) FROM t_04249_dst;
DROP TABLE t_04249_src;
DROP TABLE t_04249_dst;
EOF
