#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "SELECT uniqExact(CAST(number, 'String'))
FROM numbers(1000.)
GROUP BY CAST(number % 2, 'String')
SETTINGS max_bytes_ratio_before_external_group_by = nan;"
