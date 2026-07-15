#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: MsgPack format is not supported in fast test

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Schema inference over a MsgPack value with an empty string/bin field passed a
# null data pointer to memcpy in msgpack create_object_visitor, which is UB under
# the nonnull attribute (caught by UBSan) even when the size is 0. Raw bytes are
# piped through stdin so the test is safe to run in parallel with itself.

# Empty string field (0xa0 = fixstr of length 0): the data pointer is null.
printf '\xa0' \
  | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
      -q "SELECT toTypeName(c1), c1 FROM table"

# Empty binary field (0xc4 0x00 = bin8 of length 0).
printf '\xc4\x00' \
  | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
      -q "SELECT toTypeName(c1), c1 FROM table"

# Empty string followed by a non-empty one (the copy path must still work):
# 0xa0 = "", 0xa3 0x61 0x62 0x63 = "abc".
printf '\xa0\xa3\x61\x62\x63' \
  | $CLICKHOUSE_LOCAL --input-format MsgPack --input_format_msgpack_number_of_columns=1 \
      -q "SELECT c1 FROM table ORDER BY c1"
