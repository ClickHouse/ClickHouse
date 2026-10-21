#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The `api_key` of an AI named collection is sent as an HTTP header, so CR or LF in it must be rejected
# before any request. Port 1 refuses at once, so an unchecked key fails with a network error instead.
OPENAI="ai_crlf_openai_${CLICKHOUSE_DATABASE}"
ANTHROPIC="ai_crlf_anthropic_${CLICKHOUSE_DATABASE}"
trap '$CLICKHOUSE_CLIENT -m -q "DROP NAMED COLLECTION IF EXISTS ${OPENAI}; DROP NAMED COLLECTION IF EXISTS ${ANTHROPIC};"' EXIT

$CLICKHOUSE_CLIENT -m -q "
    CREATE NAMED COLLECTION ${OPENAI} AS
        provider = 'openai', endpoint = 'http://127.0.0.1:1/v1/chat/completions', model = 'm', api_key = 'key\r\nX-Injected: 1';
    CREATE NAMED COLLECTION ${ANTHROPIC} AS
        provider = 'anthropic', endpoint = 'http://127.0.0.1:1/v1/messages', model = 'm', api_key = 'key\nX-Injected: 1';
"

$CLICKHOUSE_CLIENT -m -q "
    SET allow_experimental_ai_functions = 1;
    SET ai_function_max_retries = 0;
    SELECT aiGenerate('hi', map('credentials', '${OPENAI}')); -- { serverError BAD_ARGUMENTS }
    SELECT aiEmbed('hi', 'm', map('credentials', '${OPENAI}')); -- { serverError BAD_ARGUMENTS }
    SELECT aiGenerate('hi', map('credentials', '${ANTHROPIC}')); -- { serverError BAD_ARGUMENTS }
    SELECT 'OK';
"
