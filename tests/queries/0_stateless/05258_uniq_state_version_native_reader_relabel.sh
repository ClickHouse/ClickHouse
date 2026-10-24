#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A version `0` `uniq` state (as written by an older server) read into a header that declares
# `AggregateFunction(1, uniq, UInt64)`: the column must take the header's version, otherwise
# an arena round trip (`groupArray`) serializes the states as version `0` and reads them back as version `1`.

$CLICKHOUSE_LOCAL -q "SELECT CAST(uniqState(number), 'AggregateFunction(uniq, UInt64)') AS s FROM numbers(100) FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format Native --structure 's AggregateFunction(1, uniq, UInt64)' \
        -q "SELECT toTypeName(s), finalizeAggregation(s), arrayMap(x -> finalizeAggregation(x), groupArray(s)) FROM table GROUP BY ALL"

# The same for a state nested in a container type.
$CLICKHOUSE_LOCAL -q "SELECT [CAST(uniqState(number), 'AggregateFunction(uniq, UInt64)')] AS s FROM numbers(100) FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format Native --structure 's Array(AggregateFunction(1, uniq, UInt64))' \
        -q "SELECT toTypeName(s), arrayMap(x -> arrayMap(y -> finalizeAggregation(y), x), groupArray(s)) FROM table GROUP BY ALL"
