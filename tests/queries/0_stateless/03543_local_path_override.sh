#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel - spawning bunch of processes with sanitizers can use significant amount of memory

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

local_path="${CUR_DIR:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
# create some existing data
$CLICKHOUSE_LOCAL --path "$local_path" -q "CREATE TABLE x (key Int) Engine=Log()"
$CLICKHOUSE_LOCAL --path "$local_path" -q "INSERT INTO x VALUES (1)"
$CLICKHOUSE_LOCAL --path "$local_path" -q "SELECT * FROM x"

config="$CUR_DIR/${CLICKHOUSE_TEST_UNIQUE_NAME}.config.xml"
cat > "$config" <<EOL
<clickhouse>
  <path>/var/lib/clickhouse</path>
</clickhouse>
EOL
$CLICKHOUSE_LOCAL --config "$config" --path "$local_path" -q "SELECT * FROM x"

rm -fr "${local_path:?}" "${config:?}"
