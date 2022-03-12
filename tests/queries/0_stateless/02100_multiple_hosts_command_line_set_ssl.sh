#!/usr/bin/env bash
# Tags: use-ssl

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

not_alive_host="10.100.0.0"
not_alive_port="1"

error="$(${CLICKHOUSE_CLIENT} --secure --host "${not_alive_host}" --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fq "DB::NetException" && echo 1 || (echo ${error} | head -1)
echo "${error}" | grep -Fq "${not_alive_host}:9440" && echo 1 || (echo ${error} | head -1)

echo '=== Values form config'

CUSTOM_CONFIG="$CURDIR/02100_config_2.xml"
rm -f ${CUSTOM_CONFIG}

cat << EOF > ${CUSTOM_CONFIG}
<config>
  <host>${not_alive_host}</host>
  <port>${not_alive_port}</port>
</config>
EOF

error="$(${CLICKHOUSE_CLIENT} --secure --config ${CUSTOM_CONFIG} --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fq "DB::NetException" && echo 1 || (echo ${error} | head -1)
echo "${error}" | grep -Fq "${not_alive_host}:${not_alive_port}" && echo 1 || (echo ${error} | head -1)

error="$(${CLICKHOUSE_CLIENT} --secure --host localhost --config ${CUSTOM_CONFIG} --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fc "DB::NetException"
echo "${error}" | grep -Fc "localhost:${not_alive_port}"

rm -f ${CUSTOM_CONFIG}

echo '=== Values form config 2'

cat << EOF > ${CUSTOM_CONFIG}
<config>
  <host>${not_alive_host}</host>
</config>
EOF

error="$(${CLICKHOUSE_CLIENT} --secure --config ${CUSTOM_CONFIG} --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fq "DB::NetException" && echo 1 || (echo ${error} | head -1)
echo "${error}" | grep -Fq "${not_alive_host}:9440" && echo 1 || (echo ${error} | head -1)

rm -f ${CUSTOM_CONFIG}
