#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

enabled=$($CLICKHOUSE_CLIENT --query "SELECT countIf(value IN ('1', 'ON')) FROM system.build_options WHERE name = 'USE_AHO_CORASICK'")
if [[ "$enabled" == "1" ]]; then
    exit 0
fi

table=multisearch_daachorse_disabled_text_index
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS $table;
CREATE TABLE $table
(
    id UInt32,
    str String,
    INDEX idx str TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO $table VALUES (1, 'hello world');
EOF

check_not_implemented()
{
    if ! $CLICKHOUSE_CLIENT --query "$1" 2>&1 | grep -q 'NOT_IMPLEMENTED'; then
        echo "Expected NOT_IMPLEMENTED: $1"
        return 1
    fi
}

for use_index in 0 1; do
    check_not_implemented "
        SELECT count() FROM $table
        WHERE multiSearchAny(str, arrayMap(x -> 'abc' || toString(x), range(256)))
        SETTINGS use_skip_indexes = $use_index"
done

for use_index in 0 1; do
    check_not_implemented "
        SELECT count() FROM $table
        WHERE multiSearchAny(str, ['missing'])
        SETTINGS use_skip_indexes = $use_index, force_daachorse_for_multi_search = 1"
done

$CLICKHOUSE_CLIENT --query "DROP TABLE $table"
