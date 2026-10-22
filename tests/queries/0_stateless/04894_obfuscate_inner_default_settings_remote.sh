#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `remote(..., view(SELECT ...))` serializes the view query through `QueryNode::toAST`.
# The inner reset must survive that conversion and override the propagated invalid
# session setting on the remote server.
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
SET enable_analyzer = 1;
SET obfuscate_markov_order = 0;
SELECT count()
FROM remote('127.0.0.2', view(
    SELECT * FROM obfuscate(SELECT 'hello world' AS s FROM numbers(8)) LIMIT 8
    SETTINGS obfuscate_markov_order = DEFAULT));
EOF
