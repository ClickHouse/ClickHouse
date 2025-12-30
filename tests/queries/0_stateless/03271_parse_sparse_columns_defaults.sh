#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS test_default_bool;

    CREATE TABLE test_default_bool (id Int8, b Bool DEFAULT false)
    ENGINE = MergeTree ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
"

echo 'INSERT INTO test_default_bool FORMAT CSV 1,\N' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @-
echo 'INSERT INTO test_default_bool FORMAT CSV 2,\N' | $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" --data-binary @-

$CLICKHOUSE_CLIENT --query "
    SELECT * FROM test_default_bool ORDER BY id;
    SELECT name, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test_default_bool' AND column = 'b' AND active;
    DROP TABLE test_default_bool;
"
