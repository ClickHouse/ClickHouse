#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The exception message contains instruction on how to reset the password:

$CLICKHOUSE_CLIENT --password incorrect-password --query "SELECT 1" 2>&1 | grep -o 'password is incorrect'
$CLICKHOUSE_CLIENT --password incorrect-password --query "SELECT 1" 2>&1 | grep -o -P 'reset.+password'

$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -u default:incorrect-password -d "SELECT 1" | grep -o 'password is incorrect'
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -u default:incorrect-password -d "SELECT 1" | grep -o -P 'reset.+password'
