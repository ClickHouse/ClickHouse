#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    SELECT reinterpretAsUInt64(assumeNotNull(dynamicElement(arrayElement(tupleElement(value, 1), 1), 'Float64')))
    FROM
    (
        SELECT tuple(if(number = 0, [CAST(-0.0, 'Dynamic')], [CAST(0.0, 'Dynamic')])) AS value
        FROM numbers(2)
    )
    WHERE value = tuple([CAST(0.0, 'Dynamic')])
    SETTINGS optimize_constant_columns_after_filter = 1
"
