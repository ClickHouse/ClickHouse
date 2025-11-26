#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS mv_table;

CREATE TABLE source_table
(
    date Date
)
ENGINE = Null;

CREATE MATERIALIZED VIEW mv_table
(
    date Date,
    number UInt32
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (date, number)
AS
SELECT
    date
FROM source_table;

INSERT INTO source_table VALUES ('2000-01-01');

CREATE USER user_${CLICKHOUSE_TEST_UNIQUE_NAME};
GRANT SELECT(date) ON source_table TO user_${CLICKHOUSE_TEST_UNIQUE_NAME};
GRANT SELECT ON mv_table TO user_${CLICKHOUSE_TEST_UNIQUE_NAME};
"

$CLICKHOUSE_CLIENT --query "SELECT * FROM mv_table;"
$CLICKHOUSE_CLIENT --user=user_${CLICKHOUSE_TEST_UNIQUE_NAME} --query "SELECT date, number FROM mv_table;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER user_${CLICKHOUSE_TEST_UNIQUE_NAME};
DROP TABLE mv_table;
DROP TABLE source_table;
"
