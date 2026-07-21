#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A well-formed timestamp parses fine (EXPLAIN SYNTAX only parses, no execution).
$CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX SYSTEM TEST VIEW v SET FAKE TIME '2024-06-01 00:00:00'" >/dev/null && echo "valid accepted"

# A valid timestamp followed by junk was silently accepted before; assertEOF now rejects it.
$CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX SYSTEM TEST VIEW v SET FAKE TIME '2024-06-01 00:00:00 junk'" 2>&1 \
    | grep -q "CANNOT_PARSE_INPUT_ASSERTION_FAILED" && echo "rejected junk"

# '2024 April 4' must not be silently reinterpreted as the timestamp 2024 (any parse error is fine).
$CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX SYSTEM TEST VIEW v SET FAKE TIME '2024 April 4'" 2>&1 \
    | grep -q "CANNOT_PARSE" && echo "rejected 2024 April 4"
