#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

diff <($CLICKHOUSE_LOCAL -q "SELECT * from system.functions") \
        <($CLICKHOUSE_LOCAL -q "SHOW FUNCTIONS")

diff <($CLICKHOUSE_LOCAL -q "SELECT * FROM system.functions WHERE name ILIKE 'quantile%'") \
        <($CLICKHOUSE_LOCAL -q "SHOW FUNCTIONS ILIKE 'quantile%'")

diff <($CLICKHOUSE_LOCAL -q "SELECT * FROM system.functions WHERE name LIKE 'median%'") \
	<($CLICKHOUSE_LOCAL -q "SHOW FUNCTIONS LIKE 'median%'")
