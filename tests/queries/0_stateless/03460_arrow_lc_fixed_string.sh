#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --output_format_arrow_low_cardinality_as_dictionary=1 "select 'a'::LowCardinality(FixedString(1)) as c1 format Arrow" | $CLICKHOUSE_LOCAL --input-format=Arrow --table=test -q "desc test; select * from test"

