#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

if [[ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.table_functions WHERE name IN ('lanceLocal', 'lanceS3', 'lanceS3Cluster')")" != "3" ]]; then
    echo "@@SKIP@@ this build does not include the local and S3 Lance table functions"
    exit 0
fi

. "${CUR_DIR}/data_lance/run_local_test.sh"
run_lance_local_test "05137_lance_experimental_gate"
