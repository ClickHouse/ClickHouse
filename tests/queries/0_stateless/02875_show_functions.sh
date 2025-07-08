#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

diff -q <($CLICKHOUSE_CLIENT -q "SELECT * from system.functions") \
        <($CLICKHOUSE_CLIENT -q "SHOW FUNCTIONS")

diff -q <($CLICKHOUSE_CLIENT -q "SELECT * FROM system.functions WHERE name ILIKE 'quantile%'") \
        <($CLICKHOUSE_CLIENT -q "SHOW FUNCTIONS ILIKE 'quantile%'")

diff -q <($CLICKHOUSE_CLIENT -q "SELECT * FROM system.functions WHERE name LIKE 'median%'") \
	<($CLICKHOUSE_CLIENT -q "SHOW FUNCTIONS LIKE 'median%'")
