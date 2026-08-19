#!/usr/bin/env bash
# A full-definition `ATTACH TABLE t UUID '...' (...) ENGINE = MergeTree ...` is CREATE-like user input,
# not a load of previously validated metadata (only the short `ATTACH TABLE t` re-attach and server
# startup are). It must therefore be validated like `CREATE TABLE`: a strict session must not be able to
# attach a TTL whose expression can only be built leniently, because the first strict rebuild of the TTL
# (an `INSERT`, a background TTL merge) would throw.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A full-definition ATTACH in an Atomic database requires an explicit UUID; generate a random one
# to avoid collisions between concurrent runs of this test.
UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# `length` has no compatible alternative inside this `Variant`, so a strict build throws. The `ATTACH`
# fails, so nothing is registered under the UUID.
# -m1 because the error message contains the error code name multiple times.
$CLICKHOUSE_CLIENT --variant_throw_on_type_mismatch 1 --allow_suspicious_ttl_expressions 0 -q "
ATTACH TABLE test_ttl_full_definition_attach UUID '${UUID}'
(
    key UInt64,
    v Variant(AggregateFunction(max, UInt64)),
    d DateTime
)
ENGINE = MergeTree()
ORDER BY key
TTL d + INTERVAL 1 DAY DELETE WHERE isNull(length(v));" 2>&1 | grep -m 1 -o -F 'ILLEGAL_TYPE_OF_ARGUMENT'

echo 'rejected'
