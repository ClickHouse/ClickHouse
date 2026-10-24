#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `--logger.level` was accepted by `clickhouse local` but overwritten with `trace`, so `--logger.console=1 --logger.level=fatal`
# still printed every trace message. `--log-level` and `--logger.level` must behave the same.

for opt in --log-level --logger.level; do
    echo "$opt=fatal"
    ${CLICKHOUSE_LOCAL} --logger.console=1 "$opt"=fatal --query "SELECT 1" 2>&1 | grep -c '<Trace>\|<Debug>\|<Information>'
    echo "$opt=trace"
    ${CLICKHOUSE_LOCAL} --logger.console=1 "$opt"=trace --query "SELECT 1" 2>&1 | grep -c '<Trace>' | sed 's/^[1-9][0-9]*$/many/'
done

# The level from a config file is honoured too (it used to be overwritten with `send_logs_level`, default trace),
# and `--log-level` overrides both it and `send_logs_level`.
CONFIG_DIR=${CLICKHOUSE_TMP}/05235_logger_level_config
mkdir -p "${CONFIG_DIR}"
cat > "${CONFIG_DIR}/config.xml" <<EOF2
<clickhouse><logger><level>trace</level><console>1</console></logger></clickhouse>
EOF2
echo "config trace, send_logs_level=fatal"
${CLICKHOUSE_LOCAL} --config-file="${CONFIG_DIR}/config.xml" --send_logs_level=fatal --query "SELECT 1" 2>&1 | grep -c '<Trace>' | sed 's/^[1-9][0-9]*$/many/'
echo "config trace, --log-level=none"
${CLICKHOUSE_LOCAL} --config-file="${CONFIG_DIR}/config.xml" --log-level=none --query "SELECT 1" 2>&1 | grep -c '<Trace>\|<Debug>\|<Information>'
sed -i 's/trace/fatal/' "${CONFIG_DIR}/config.xml"
echo "config fatal, send_logs_level=fatal"
${CLICKHOUSE_LOCAL} --config-file="${CONFIG_DIR}/config.xml" --send_logs_level=fatal --query "SELECT 1" 2>&1 | grep -c '<Trace>\|<Debug>\|<Information>'
echo "config fatal, --log-level=trace"
${CLICKHOUSE_LOCAL} --config-file="${CONFIG_DIR}/config.xml" --log-level=trace --query "SELECT 1" 2>&1 | grep -c '<Trace>' | sed 's/^[1-9][0-9]*$/many/'
rm -rf "${CONFIG_DIR}"
