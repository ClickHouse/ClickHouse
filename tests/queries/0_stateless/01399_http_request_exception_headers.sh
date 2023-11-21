#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "-- Non-retryable exceptions"
RES=$(curl -vs http://localhost:8123 -d 'SELECT x' 2>&1)
echo $RES | grep -c '< X-ClickHouse-Exception-Code: 47'
echo $RES | grep -c '< X-ClickHouse-Exception-Retryable' || true
RES=$(curl -vs http://localhost:8123 -d 'SELECT someUnknownFunction(42)' 2>&1)
echo $RES | grep -c '< X-ClickHouse-Exception-Code: 46'
echo $RES | grep -c '< X-ClickHouse-Exception-Retryable' || true

echo "-- Retryable exceptions"
declare -a RETRYABLE_EXCEPTION_CODES=(3 159 164 202 203 209 210 242 252 285 319 425 999 1002)
for code in "${RETRYABLE_EXCEPTION_CODES[@]}"
do
  echo "-- Code: $code"
  RES=$(curl -vs http://localhost:8123 -d "select throwIf(1, 'test', toInt16($code)) settings allow_custom_error_code_in_throwif=1" 2>&1)
  echo $RES | grep -c "X-ClickHouse-Exception-Code: $code"
  echo $RES | grep -c '< X-ClickHouse-Exception-Retryable: 1'
done
