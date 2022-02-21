#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# default values test
${CLICKHOUSE_CLIENT} --query "SELECT 1"

echo '=== Backward compatibility test'
${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --query "SELECT 1";

echo '=== Cannot resolve host'
not_resolvable_host="notlocalhost"
error="$(${CLICKHOUSE_CLIENT} --host "${not_resolvable_host}" --query "SELECT 1" 2>&1 > /dev/null)";
echo "${error}" | grep -Fc "DNS_ERROR"
echo "${error}" | grep -Fq "${not_resolvable_host}" && echo 1 || echo 0

echo '=== Bad arguments'
not_number_port="abc"
error="$(${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --port "${not_number_port}" --query "SELECT 1" 2>&1 > /dev/null)";
echo "${error}" | grep -Fc "Bad arguments"
echo "${error}" | grep -Fc "${not_number_port}"

echo '=== Not alive host'

not_alive_host="10.100.0.0"
${CLICKHOUSE_CLIENT} --host "${not_alive_host}" --host "${CLICKHOUSE_HOST}" --query "SELECT 1";

not_alive_port="1"
error="$(${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --port "${not_alive_port}" --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fc "Code: 210"
echo "${error}" | grep -Fc "${CLICKHOUSE_HOST}:${not_alive_port}"

${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --port "${not_alive_port}" --host "${CLICKHOUSE_HOST}" --query "SELECT 1";
${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --port "${not_alive_port}" --query "SELECT 1";

echo '=== Code 210 with ipv6'

ipv6_host_without_brackets="2001:3984:3989::1:1000"
error="$(${CLICKHOUSE_CLIENT} --host "${ipv6_host_without_brackets}" --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fc "Code: 210"
echo "${error}" | grep -Fc "${ipv6_host_without_brackets}"

ipv6_host_with_brackets="[2001:3984:3989::1:1000]"

error="$(${CLICKHOUSE_CLIENT} --host "${ipv6_host_with_brackets}" --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fc "Code: 210"
echo "${error}" | grep -Fc "${ipv6_host_with_brackets}"

error="$(${CLICKHOUSE_CLIENT} --host "${ipv6_host_with_brackets}" --port "${not_alive_port}" --query "SELECT 1" 2>&1 > /dev/null)"
echo "${error}" | grep -Fc "Code: 210"
echo "${error}" | grep -Fc "${ipv6_host_with_brackets}:${not_alive_port}"

echo '==='

${CLICKHOUSE_CLIENT} --query "SELECT 1";
${CLICKHOUSE_CLIENT} --port "${CLICKHOUSE_PORT_TCP}" --query "SELECT 1";
${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --query "SELECT 1";
${CLICKHOUSE_CLIENT} --port "${CLICKHOUSE_PORT_TCP}" --host "${CLICKHOUSE_HOST}" --query "SELECT 1";
${CLICKHOUSE_CLIENT} --port "${CLICKHOUSE_PORT_TCP}" --host "${CLICKHOUSE_HOST}" --host "{$not_alive_host}" --port "${CLICKHOUSE_PORT_TCP}" --query "SELECT 1";
${CLICKHOUSE_CLIENT} --port "${CLICKHOUSE_PORT_TCP}" --host "{$not_alive_host}" --host "${CLICKHOUSE_HOST}" --query "SELECT 1" 2> /dev/null;
${CLICKHOUSE_CLIENT} --port "${CLICKHOUSE_PORT_TCP}"  --port "${CLICKHOUSE_PORT_TCP}" --port "${CLICKHOUSE_PORT_TCP}" --host "{$not_alive_host}" --host "${CLICKHOUSE_HOST}" --query "SELECT 1";

