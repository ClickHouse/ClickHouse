#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# A numeric value that schema inference widened to `Int64` / `Float64` is accepted by the text / JSON
# deserializers into most scalar destinations (a `DateTime` / `Date` timestamp, an `Enum` value, `Decimal`,
# ...), so the schema-mismatch diagnostic treats a number as compatible with them. But `UUID`, `IPv4` and
# `IPv6` deserializers require a (quoted) string and reject a bare number in every format, so a number there
# really is a structure mismatch the parser rejects and the explanation must be attached. `FixedString` is
# the exception: `TSV` / `CSV` read the raw field verbatim into it, so a number is valid and must not be
# flagged.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- JSONEachRow, a bare number into a UUID column is a genuine structure mismatch"
printf 'CREATE TABLE t (u UUID) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"u": 1}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, a bare number into an IPv4 column is a genuine structure mismatch"
printf 'CREATE TABLE t (ip IPv4) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"ip": 1}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow, a bare number into an IPv6 column is a genuine structure mismatch"
printf 'CREATE TABLE t (ip IPv6) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow\n{"ip": 1}\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- TSV, a number into a FixedString column is read verbatim; a bad numeric column elsewhere must not add a false positive"
printf 'CREATE TABLE t (f FixedString(3), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t1.5\n' \
    | $CLICKHOUSE_LOCAL 2>&1 | check
