#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    SELECT reinterpretAsUInt64(tupleElement(arrayElement(value, 1), 1))
    FROM
    (
        SELECT if(number = 0, [tuple(CAST(-0.0, 'Float64'))], [tuple(CAST(0.0, 'Float64'))]) AS value
        FROM numbers(2)
    )
    WHERE value = [tuple(CAST(0.0, 'Float64'))]
    SETTINGS optimize_constant_columns_after_filter = 1
"
