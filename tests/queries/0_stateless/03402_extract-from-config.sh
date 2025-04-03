#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Check for scalar values
$CLICKHOUSE_EXTRACT_CONFIG --key=tcp_port

# Check for XML values
$CLICKHOUSE_EXTRACT_CONFIG --key=logger

# Check for array values
$CLICKHOUSE_EXTRACT_CONFIG --key=listen_host

# Check for nested arrays and -o flag
$CLICKHOUSE_EXTRACT_CONFIG --key='http_options_response' --output=extracted_config.xml
cat extracted_config.xml

# Check that there is no exception when --try is provided
$CLICKHOUSE_EXTRACT_CONFIG --key=non-existent-key --try
echo $?

#Check that entire config is printed when no key is provided TODO:
#$CLICKHOUSE_EXTRACT_CONFIG

# Cleanup
rm extracted_config.xml