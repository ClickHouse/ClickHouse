#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: require SSL

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_path=$(mktemp "$CUR_DIR/$(basename "${BASH_SOURCE[0]}" ".sh")-XXXXXX.yaml")
touch $config_path

# Give the TLS handshake a generous budget: the default 10 s connect_timeout /
# handshake_timeout_ms can fire on slow lanes (amd_tsan, s3, parallel) before
# certificate verification runs, leaking a SOCKET_TIMEOUT into the output.
BENCHMARK_TIMEOUTS="--connect_timeout 60 --handshake_timeout_ms 60000"

$CLICKHOUSE_BENCHMARK $BENCHMARK_TIMEOUTS --config $config_path -q "select 1" -i 1 --secure |& grep -m1 -F -o -e "certificate verify failed" || {
  echo "--secure should require --accept-invalid-certificate" >&2
  $CLICKHOUSE_BENCHMARK $BENCHMARK_TIMEOUTS --config $config_path -q "select 1" -i 1 --secure >&2
}
$CLICKHOUSE_BENCHMARK $BENCHMARK_TIMEOUTS --config $config_path -q "select 1" -i 1 --secure --accept-invalid-certificate |& grep -F -e Exception

rm -f "${config_path:?}"
exit 0
