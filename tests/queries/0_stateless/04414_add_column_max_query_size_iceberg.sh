#!/usr/bin/env bash
# Tags: no-fasttest, s3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/79887
# ADD COLUMN on Iceberg writes a new metadata file to object storage before the
# max_query_size check fires in alterTable, so the external schema is mutated
# even though the ALTER is rejected.
sub_path="iceberg_max_query_size_$RANDOM"
cols=$($CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(arrayMap(i -> 'c' || toString(i) || ' Int32', range(200)), ', ')")
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ice SYNC"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ice (${cols}) ENGINE = Iceberg('http://localhost:11111/test/${sub_path}/', 'clickhouse', 'clickhouse')"

# ADD COLUMN must be rejected before the Iceberg metadata file is written.
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 --max_query_size=2825 -q "ALTER TABLE ice ADD COLUMN col_new Nullable(Int32)" 2>&1 | grep -o -F -m1 "QUERY_IS_TOO_LARGE"

# The rejected ALTER must not have written v2.metadata.json to object storage.
if $CLICKHOUSE_CLIENT -q "SELECT count() FROM s3('http://localhost:11111/test/${sub_path}/metadata/v2.metadata.json', 'clickhouse', 'clickhouse', 'LineAsString')" >/dev/null 2>&1; then
    echo "v2_exists"
else
    echo "v2_absent"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ice SYNC"
