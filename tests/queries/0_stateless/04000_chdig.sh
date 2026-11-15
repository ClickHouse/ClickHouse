#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_BINARY chdig --version | grep -o chdig
