#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SQL_FILE_NAME=$"03156_default_multiquery_split_${CLICKHOUSE_DATABASE}.sql"

# The old multiquery implementation uses '\n' to split INSERT query segmentation
# this case is mainly to test the following situations
# 1. INSERT(format=Values) query: split by ';'
# 2. INSERT(format is not Values) query: split by '\n\n' instead of ';', then discard the ramaining data
# 3. INSERT(format is not Values) query: split by '\n\n', causing the trailing ';'' to be treated as the part of last value
# 4. The client uses multiquery by default, regardless of whether multiquery option is used.

# create table test1, test2, then
# 1. insert 101, 102 into test1
# 2. insert 1, 2; into test2, ';' will be treated as a part of a value
# 3. insert 3, 4; '6' will be treated as the next query because of the empty line, we use empty line to determine the end of insert query(format IS NOT VALUES)
#     '6' will cause Syntax error
cat << EOF > "$SQL_FILE_NAME"
DROP TABLE IF EXISTS TEST1;
DROP TABLE IF EXISTS TEST2;
CREATE TABLE TEST1 (value Float64) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE TEST2 (value String)  ENGINE=MergeTree ORDER BY tuple();
INSERT INTO TEST1 VALUES
(101),
(102);
INSERT INTO TEST2 FORMAT CSV
1
2;

INSERT INTO TEST2 FORMAT CSV
3
4

6
EOF

$CLICKHOUSE_CLIENT -m < "$SQL_FILE_NAME" 2>&1 | grep -o 'Syntax error'

# insert 7, 8, 9 into test2, because we use semicolon to determine the end of insert query(format is VALUES)
# then select all data from test1 and test2
cat << EOF > "$SQL_FILE_NAME"
INSERT INTO TEST2 VALUES
('7'),
('8'),

('9');

SELECT * FROM TEST1 ORDER BY value;
SELECT * FROM TEST2 ORDER BY value;
DROP TABLE TEST1; DROP TABLE TEST2;
EOF

$CLICKHOUSE_CLIENT -m -n < "$SQL_FILE_NAME"

rm "$SQL_FILE_NAME"
