#!/usr/bin/env bash
# Tags: long, no-debug

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 "$CUR_DIR"/04402_clickhouse_client_autocomplete_inline_hints.python
