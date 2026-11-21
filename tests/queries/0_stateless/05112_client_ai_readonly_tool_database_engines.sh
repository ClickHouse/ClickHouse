#!/usr/bin/env bash
# Tags: long, no-debug, no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 "$CUR_DIR"/05112_client_ai_readonly_tool_database_engines.python
