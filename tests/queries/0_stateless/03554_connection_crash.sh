#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="$USER_FILES_PATH/$CLICKHOUSE_DATABASE.data"

# Test from https://github.com/ClickHouse/ClickHouse/issues/83123
$CLICKHOUSE_CLIENT --ignore-error -nm -q "
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 JSON) ENGINE = MergeTree() PRIMARY KEY tuple();
CREATE TABLE t1 (c0 Bool, c1 FixedString(42)) ENGINE = Buffer(currentDatabase(), t0, 71, 5, 379, 74, 221, 238, 858, 34, 55);
SET min_insert_block_size_bytes = 4;
INSERT INTO TABLE t1 (c0, c1) SETTINGS optimize_trivial_insert_select = 1 SELECT c0, c1 FROM generateRandom('c0 Bool, c1 FixedString(42)', 9090224004680416777, 6350, 9) LIMIT 187;
SET input_format_max_block_size_bytes = 4, min_insert_block_size_rows = 32, min_insert_block_size_bytes = 0;
INSERT INTO TABLE FUNCTION file('$path', 'CSV', 'c1 FixedString(42), c0 Bool') SELECT c1, c0 FROM t1 FINAL;
INSERT INTO TABLE t1 (c1, c0) FROM INFILE '$path' FORMAT CSV; -- { serverError TYPE_MISMATCH }
INSERT INTO TABLE t1 (c1, c0) VALUES ('move', TRUE);
SELECT 1;
"
