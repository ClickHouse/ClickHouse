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

QUERIES="
CREATE NAMED COLLECTION ai_sql_chat AS
    provider = 'openai', endpoint = 'https://ai.example.com/v1/chat/completions', model = 'chat-model', api_key = 'fake-key';
CREATE NAMED COLLECTION ai_sql_embed AS
    provider = 'openai', endpoint = 'https://ai.example.com/v1/embeddings', api_key = 'fake-key';
CREATE TABLE tab (x String) ENGINE = Memory;

SELECT '-- config collection, credentials parameter';
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_config_chat')) AS r FROM tab);
SELECT count() FROM (SELECT aiEmbed(x, 'embed-model', map('credentials', 'ai_config_embed')) AS r FROM tab);
SELECT count() FROM (SELECT aiSimilarity(x, x, 'embed-model', map('credentials', 'ai_config_embed')) AS r FROM tab);

SELECT '-- SQL collection, credentials parameter';
SELECT count() FROM (SELECT aiGenerate(x, map('credentials', 'ai_sql_chat')) AS r FROM tab);
SELECT count() FROM (SELECT aiEmbed(x, 'embed-model', map('credentials', 'ai_sql_embed')) AS r FROM tab);
SELECT count() FROM (SELECT aiSimilarity(x, x, 'embed-model', map('credentials', 'ai_sql_embed')) AS r FROM tab);

SELECT '-- config collection, default-credentials settings';
SET ai_function_text_default_credentials = 'ai_config_chat', ai_function_embedding_default_credentials = 'ai_config_embed';
SELECT count() FROM (SELECT aiGenerate(x) AS r FROM tab);
SELECT count() FROM (SELECT aiEmbed(x, 'embed-model') AS r FROM tab);
SELECT count() FROM (SELECT aiSimilarity(x, x, 'embed-model') AS r FROM tab);

SELECT '-- SQL collection, default-credentials settings';
SET ai_function_text_default_credentials = 'ai_sql_chat', ai_function_embedding_default_credentials = 'ai_sql_embed';
SELECT count() FROM (SELECT aiGenerate(x) AS r FROM tab);
SELECT count() FROM (SELECT aiEmbed(x, 'embed-model') AS r FROM tab);
SELECT count() FROM (SELECT aiSimilarity(x, x, 'embed-model') AS r FROM tab);
"

# Prints result rows as they are and replaces each error with its code name.
function run()
{
    $CLICKHOUSE_LOCAL --config-file <(config "$1") --ignore-error -q "$QUERIES" 2>&1 \
        | grep -v -e '^Received exception:$' -e '^(query: ' \
        | sed -E 's/.*Code: [0-9]+\. DB::Exception: .*\(([A-Z_]+)\)$/\1/'
}

echo "== default"
run ""
echo "== ai_function_allow_sql_named_collections = 0"
run "<ai_function_allow_sql_named_collections>0</ai_function_allow_sql_named_collections>"
