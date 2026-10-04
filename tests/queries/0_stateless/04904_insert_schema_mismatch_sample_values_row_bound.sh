#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Schema inference is limited to the first row because that is where the real parser fails.
# The sampled-value reparse must obey that same limit: the malformed second row must not prevent
# the first row's invalid `Bool` literal from producing the structure-mismatch diagnostic.
{
    echo "SET input_format_max_rows_to_read_for_schema_inference = 1; CREATE TABLE t (b Bool) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow"
    echo '{"b":2}'
    echo '{"b":"x"}'
} | $CLICKHOUSE_LOCAL 2>&1 | grep -F -q 'does not match the structure expected by the query' && echo 'explanation present' || echo 'explanation missing'
