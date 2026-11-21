#!/usr/bin/env bash
# Tags: long, no-debug

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 "$CUR_DIR"/04911_client_ai_mode_history_recall.python
