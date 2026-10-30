#!/usr/bin/env bash
# Tags: no-fasttest, no-cpu-s390x
# no-fasttest -- compiled w/o datasketches

# A sketch that another system wrote without a length in front of it is not a state of `uniqApacheHLL`: the state reader takes
# its first bytes for the length. `CORRUPTED_DATA` is always logged by the client, so this is a shell test, as in
# `05340_uniq_apache_hll_corrupted_state.sh`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SKETCH="unhex('0201070b03080508cbd7c2042bf2fb06862ff90d7581660781bc5d06')"

$CLICKHOUSE_CLIENT --query \
    "SELECT finalizeAggregation(CAST($SKETCH, 'AggregateFunction(uniqApacheHLL, UInt64)'))" 2>&1 \
    | grep -q -F 'CORRUPTED_DATA' && echo 'OK without the length' || echo 'FAIL without the length'

$CLICKHOUSE_CLIENT --query \
    "SELECT finalizeAggregation(CAST(addVarUIntLengthPrefix($SKETCH), 'AggregateFunction(uniqApacheHLL, UInt64)'))" 2>&1
