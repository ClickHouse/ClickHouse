#!/usr/bin/env bash
# `__getScalar` is internal, but it can be written by hand. When its name argument is wrapped, the
# reference must be rejected with an error instead of aborting the server, and a plain reference must
# keep working.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A two-column scalar subquery stays a scalar reference in the query tree, and one above 1 MiB is not
# folded back into a literal.
SUB="SELECT repeat('xy', 600000), 'folded_marker'"

HASH=$($CLICKHOUSE_CLIENT -q "SELECT extract(explain, '[0-9]+_[0-9]+') FROM (EXPLAIN QUERY TREE SELECT ($SUB)) WHERE explain LIKE '%constant_value:%'" | head -1)
if [ -z "$HASH" ]; then echo "FAILED: no scalar reference in the query tree"; exit 1; fi

# Plain reference: still resolved and still folded.
$CLICKHOUSE_CLIENT -q "SELECT tupleElement(($SUB), 2) AS registered, blockSerializedSize(__getScalar('$HASH')) > 1200000 AS plain_reference_works"

# A well-formed reference is folded while the query is analysed, not merely executed at run time.
$CLICKHOUSE_CLIENT -q "SELECT countIf(explain ILIKE '%constant_value: \'folded_marker\'%') > 0 FROM (EXPLAIN QUERY TREE SELECT tupleElement(($SUB), 2))"

# Wrapped name: an error, and the server survives it.
WRAPPED=$($CLICKHOUSE_CLIENT -q "SELECT ($SUB) AS a, blockSerializedSize(__getScalar(toNullable('$HASH'))) AS b FORMAT Null" 2>&1)
case "$WRAPPED" in *'should not be used directly'*) echo 'rejected' ;; *) echo "NOT REJECTED: $WRAPPED" ;; esac
$CLICKHOUSE_CLIENT -q "SELECT 'alive'"
