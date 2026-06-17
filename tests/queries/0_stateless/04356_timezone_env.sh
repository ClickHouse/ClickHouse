#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TZ='' ${CLICKHOUSE_LOCAL} --query "SELECT timezone()";
TZ='Asia/Shanghai' ${CLICKHOUSE_LOCAL} --query "SELECT timezone()";
TZ=':Asia/Shanghai' ${CLICKHOUSE_LOCAL} --query "SELECT timezone()";
TZ=':/usr/share/zoneinfo/Europe/Amsterdam' ${CLICKHOUSE_LOCAL} --query "SELECT timezone()";
TZ='Poland' ${CLICKHOUSE_LOCAL} --query "SELECT timezone()";
