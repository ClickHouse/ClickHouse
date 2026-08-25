#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: require SSL

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_path=$(mktemp "$CUR_DIR/$(basename "${BASH_SOURCE[0]}" ".sh")-XXXXXX.yaml")
touch $config_path

$CLICKHOUSE_BENCHMARK --config $config_path -q "select 1" -i 1 --secure |& grep -m1 -F -o -e "certificate verify failed" || {
  echo "--secure should require --accept-invalid-certificate" >&2
  $CLICKHOUSE_BENCHMARK --config $config_path -q "select 1" -i 1 --secure >&2
}
$CLICKHOUSE_BENCHMARK --config $config_path -q "select 1" -i 1 --secure --accept-invalid-certificate |& grep -F -e Exception

rm -f "${config_path:?}"
exit 0
