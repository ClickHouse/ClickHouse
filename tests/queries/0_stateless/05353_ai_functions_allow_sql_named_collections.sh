#!/usr/bin/env bash

# The server setting `ai_function_allow_sql_named_collections = 0` restricts AI functions to named
# collections defined in the server configuration file. Collections created with `CREATE NAMED COLLECTION`
# are rejected, whether passed in the `credentials` parameter or via the default-credentials settings.
# The check runs in `resolveAIParams`, before the zero-row early return, so empty inputs exercise it
# without any HTTP call.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function config()
{
    echo "<clickhouse>
        $1
        <named_collections>
            <ai_config_chat>
                <provider>openai</provider>
                <endpoint>https://ai.example.com/v1/chat/completions</endpoint>
                <model>chat-model</model>
                <api_key>fake-key</api_key>
            </ai_config_chat>
            <ai_config_embed>
                <provider>openai</provider>
                <endpoint>https://ai.example.com/v1/embeddings</endpoint>
                <api_key>fake-key</api_key>
            </ai_config_embed>
        </named_collections>
    </clickhouse>"
}

SETUP="
CREATE NAMED COLLECTION ai_sql_chat AS
    provider = 'openai', endpoint = 'https://ai.example.com/v1/chat/completions', model = 'chat-model', api_key = 'fake-key';
CREATE NAMED COLLECTION ai_sql_embed AS
    provider = 'openai', endpoint = 'https://ai.example.com/v1/embeddings', api_key = 'fake-key';
CREATE TABLE tab (x String) ENGINE = Memory;
"

# Runs one query in its own process and prints its result, or the code name of its error.
# `--ignore-error` is not used: older `clickhouse-local` versions do not print errors with it.
function query()
{
    $CLICKHOUSE_LOCAL --config-file <(config "$1") -q "$SETUP $2" 2>&1 \
        | sed -E 's/.*Code: [0-9]+\. DB::Exception: .*\(([A-Z_]+)\)$/\1/'
}

# $1 is the server config fragment, $2 and $3 are the chat and embedding collections.
function queries_with_credentials()
{
    query "$1" "SELECT count() FROM (SELECT aiGenerate(x, map('credentials', '$2')) AS r FROM tab)"
    query "$1" "SELECT count() FROM (SELECT aiEmbed(x, 'embed-model', map('credentials', '$3')) AS r FROM tab)"
    query "$1" "SELECT count() FROM (SELECT aiSimilarity(x, x, 'embed-model', map('credentials', '$3')) AS r FROM tab)"
}

function queries_with_default_credentials()
{
    local set="SET ai_function_text_default_credentials = '$2', ai_function_embedding_default_credentials = '$3';"
    query "$1" "$set SELECT count() FROM (SELECT aiGenerate(x) AS r FROM tab)"
    query "$1" "$set SELECT count() FROM (SELECT aiEmbed(x, 'embed-model') AS r FROM tab)"
    query "$1" "$set SELECT count() FROM (SELECT aiSimilarity(x, x, 'embed-model') AS r FROM tab)"
}

function run()
{
    echo "-- config collection, credentials parameter"
    queries_with_credentials "$1" ai_config_chat ai_config_embed
    echo "-- SQL collection, credentials parameter"
    queries_with_credentials "$1" ai_sql_chat ai_sql_embed
    echo "-- config collection, default-credentials settings"
    queries_with_default_credentials "$1" ai_config_chat ai_config_embed
    echo "-- SQL collection, default-credentials settings"
    queries_with_default_credentials "$1" ai_sql_chat ai_sql_embed
}

echo "== default"
run ""
echo "== ai_function_allow_sql_named_collections = 0"
run "<ai_function_allow_sql_named_collections>0</ai_function_allow_sql_named_collections>"
