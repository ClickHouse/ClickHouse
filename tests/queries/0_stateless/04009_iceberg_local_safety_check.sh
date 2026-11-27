#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --enable_time_time64_type=1 --session_timezone="UTC"  -q "desc icebergLocal('${CUR_DIR}/data_minio/simple_usage_test');"  2>&1 | grep -q 'PATH_ACCESS_DENIED' && echo "GOT ACCESS DENIED ERROR"

${CLICKHOUSE_CLIENT} --enable_time_time64_type=1 --session_timezone="UTC"  -q "select * from icebergLocal('${CUR_DIR}/data_minio/simple_usage_test');" 2>&1 | grep -q 'PATH_ACCESS_DENIED' && echo "GOT ACCESS DENIED ERROR"

