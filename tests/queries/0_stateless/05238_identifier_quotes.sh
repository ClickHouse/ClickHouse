#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

check_roundtrip()
{
    query="$1"

    formatted=$(printf '%s\n' "$query" | $CLICKHOUSE_FORMAT) || exit 1
    formatted_again=$(printf '%s\n' "$formatted" | $CLICKHOUSE_FORMAT) || exit 1

    [ "$formatted" = "$formatted_again" ] || exit 1
}

check_roundtrip 'SELECT `cube`, count() FROM t GROUP BY `cube`'
check_roundtrip 'SELECT `rollup`, count() FROM t GROUP BY `rollup`'
check_roundtrip 'WITH `recursive` AS (SELECT 1 AS x) SELECT * FROM `recursive`'

$CLICKHOUSE_CLIENT --multiquery <<'SQL'
CREATE TABLE identifier_quotes_t (`cube` UInt8, `rollup` UInt8)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO identifier_quotes_t VALUES (1, 2);

CREATE VIEW identifier_quotes_v_cube AS
SELECT `cube`, count() AS c
FROM identifier_quotes_t
GROUP BY `cube`;

DETACH TABLE identifier_quotes_v_cube;
ATTACH TABLE identifier_quotes_v_cube;

SELECT * FROM identifier_quotes_v_cube;

CREATE VIEW identifier_quotes_v_rollup AS
SELECT `rollup`, count() AS c
FROM identifier_quotes_t
GROUP BY `rollup`;

DETACH TABLE identifier_quotes_v_rollup;
ATTACH TABLE identifier_quotes_v_rollup;

SELECT * FROM identifier_quotes_v_rollup;

CREATE VIEW identifier_quotes_v_recursive AS
WITH `recursive` AS (SELECT 1 AS x)
SELECT * FROM `recursive`;

DETACH TABLE identifier_quotes_v_recursive;
ATTACH TABLE identifier_quotes_v_recursive;

SELECT * FROM identifier_quotes_v_recursive;

DROP VIEW identifier_quotes_v_cube;
DROP VIEW identifier_quotes_v_rollup;
DROP VIEW identifier_quotes_v_recursive;
DROP TABLE identifier_quotes_t;
SQL
