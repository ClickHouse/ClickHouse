#!/usr/bin/env bash
# Tags: no-fasttest, long
# long - under asan in flaky check it can take more than 180 seconds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
${CUR_DIR}/02125_lz4_compression_bug.lib JSONStringsEachRow
