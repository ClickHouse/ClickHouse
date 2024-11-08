#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# shellcheck source=./03036_dynamic_read_subcolumns.lib
. "$CUR_DIR"/03036_dynamic_read_subcolumns.lib

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --use_variant_as_common_type=1 --allow_experimental_dynamic_type=1"

$CH_CLIENT -q "drop table if exists test;"

echo "Memory"
$CH_CLIENT -q "create table test (id UInt64, d Dynamic) engine=Memory"
test
$CH_CLIENT -q "drop table test;"
