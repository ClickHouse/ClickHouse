#!/usr/bin/env bash
# Tags: no-asan, no-msan, no-fasttest
# Tag no-msan: can't pass because odbc libraries are not instrumented

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

 echo "select 1 except select 1" | isql "ClickHouse" >/dev/null

