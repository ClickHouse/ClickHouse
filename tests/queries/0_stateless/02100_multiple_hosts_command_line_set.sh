#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# default values test
${CLICKHOUSE_CLIENT} --query "SELECT 1"

# backward compatibility test
${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --query "SELECT 1";

not_resolvable_host="notlocalhost"
exception_msg="Cannot resolve host (${not_resolvable_host}), error 0: ${not_resolvable_host}.
Code: 198. DB::Exception: Not found address of host: ${not_resolvable_host}. (DNS_ERROR)
"
error="$(${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" "${not_resolvable_host}"  --query "SELECT 1" 2>&1 > /dev/null)";
[ "${error}" == "${exception_msg}" ]; echo "$?"

not_number_port="abc"
exception_msg="Bad arguments: the argument ('${CLICKHOUSE_HOST}:${not_number_port}') for option '--host' is invalid."
error="$(${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}:${not_number_port}"  --query "SELECT 1" 2>&1 > /dev/null)";
[ "${error}" == "${exception_msg}" ]; echo "$?"

not_alive_host="10.100.0.0"
${CLICKHOUSE_CLIENT} --host "${not_alive_host}" "${CLICKHOUSE_HOST}" --query "SELECT 1";

not_alive_port="1"
exception_msg="Code: 210. DB::NetException: Connection refused (${CLICKHOUSE_HOST}:${not_alive_port}). (NETWORK_ERROR)
"
error="$(${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}" --port "${not_alive_port}" --query "SELECT 1" 2>&1 > /dev/null)"
[ "${error}" == "${exception_msg}" ]; echo "$?"
${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}:${not_alive_port}" "${CLICKHOUSE_HOST}" --query "SELECT 1";
${CLICKHOUSE_CLIENT} --host "${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}" --port "${not_alive_port}" --query "SELECT 1";

