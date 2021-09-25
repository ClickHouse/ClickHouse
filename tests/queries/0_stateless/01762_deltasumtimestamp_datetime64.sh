#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT deltaSumTimestamp(1, now64());" 2>&1 | grep -q "Code: 43.*Illegal type DateTime64" && echo 'OK' || echo 'FAIL';

