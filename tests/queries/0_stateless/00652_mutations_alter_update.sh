#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS alter_update"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE alter_update \
    (d Date, key UInt32, value1 String, value2 UInt64, materialized_value String MATERIALIZED concat('materialized_', toString(value2 + 7))) \
    ENGINE MergeTree ORDER BY key PARTITION BY toYYYYMM(d)"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test expected failures ***'"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update UPDATE d = today() WHERE 1" 2>/dev/null || echo "Updating partition key should fail"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update UPDATE key = 1 WHERE 1" 2>/dev/null || echo "Updating primary key should fail"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update UPDATE materialized_value = 'aaa' WHERE 1" 2>/dev/null || echo "Updating MATERIALIZED column should fail"
${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update UPDATE value1 = 'aaa' WHERE 'string'" 2>/dev/null || echo "Updating with non-UInt8 predicate should fail"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test updating according to a predicate ***'"

${CLICKHOUSE_CLIENT} --query="INSERT INTO alter_update VALUES \
    ('2000-01-01', 123, 'abc', 1), \
    ('2000-01-01', 234, 'cde', 2)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update UPDATE value1 = 'aaa', value2 = value2 + 100 WHERE key < 200"
wait_for_mutation "alter_update" "mutation_2.txt"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM alter_update ORDER BY key"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update DROP PARTITION 200001"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test several UPDATE commands with common subexpressions ***'"

${CLICKHOUSE_CLIENT} --query="INSERT INTO alter_update VALUES ('2000-01-01', 123, 'abc', 49)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update \
    UPDATE value2 = (value2 + 1) / 2 WHERE 1, \
    UPDATE value2 = value2 + 1 WHERE 1"
wait_for_mutation "alter_update" "mutation_4.txt"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM alter_update ORDER BY key"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update DROP PARTITION 200001"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test predicates with IN operator ***'"

${CLICKHOUSE_CLIENT} --query="INSERT INTO alter_update VALUES \
    ('2000-01-01', 123, 'abc', 10), \
    ('2000-01-01', 234, 'cde', 20), \
    ('2000-01-01', 345, 'fgh', 30), \
    ('2000-01-01', 456, 'ijk', 40)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update \
    DELETE WHERE key IN (SELECT toUInt32(arrayJoin([121, 122, 123]))), \
    UPDATE value1 = concat(value1, 'ccc') WHERE value2 IN (20, 30), \
    UPDATE value1 = 'iii' WHERE value2 IN (SELECT toUInt64(40))"
wait_for_mutation "alter_update" "mutation_6.txt"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM alter_update ORDER BY key"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update DROP PARTITION 200001"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test UPDATE of columns that DELETE depends on ***'"

${CLICKHOUSE_CLIENT} --query="INSERT INTO alter_update VALUES \
    ('2000-01-01', 123, 'abc', 10), \
    ('2000-01-01', 234, 'cde', 20)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update \
    UPDATE value2 = value2 + 10 WHERE 1, \
    DELETE WHERE value2 = 20"
wait_for_mutation "alter_update" "mutation_8.txt"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM alter_update ORDER BY key"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update DROP PARTITION 200001"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test complex mixture of UPDATEs and DELETEs ***'"

${CLICKHOUSE_CLIENT} --query="INSERT INTO alter_update VALUES \
    ('2000-01-01', 123, 'abc', 10), \
    ('2000-01-01', 234, 'cde', 20), \
    ('2000-01-01', 345, 'fgh', 30), \
    ('2000-01-01', 456, 'ijk', 40)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update \
    DELETE WHERE value2 IN (8, 9, 10), \
    UPDATE value2 = value2 + 10 WHERE value2 <= 10, \
    DELETE WHERE length(value1) + value2 = 23, \
    DELETE WHERE materialized_value = 'materialized_37', \
    UPDATE value1 = concat(value1, '_', materialized_value) WHERE key = 456"
wait_for_mutation "alter_update" "mutation_10.txt"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM alter_update ORDER BY key"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update DROP PARTITION 200001"


${CLICKHOUSE_CLIENT} --query="SELECT '*** Test updating columns that MATERIALIZED columns depend on ***'"

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS materialized_key"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE materialized_key \
    (key UInt32 MATERIALIZED value + 1, value UInt32) \
    ENGINE MergeTree ORDER BY key"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE materialized_key UPDATE value = 1 WHERE 1" 2>/dev/null || echo "Updating column that MATERIALIZED key column depends on should fail"

${CLICKHOUSE_CLIENT} --query="DROP TABLE materialized_key"

${CLICKHOUSE_CLIENT} --query="INSERT INTO alter_update VALUES \
    ('2000-01-01', 123, 'abc', 10), \
    ('2000-01-01', 234, 'cde', 20), \
    ('2000-01-01', 345, 'fgh', 30), \
    ('2000-01-01', 456, 'ijk', 40)"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update \
    UPDATE value2 = value2 + 7 WHERE value2 <= 20"
wait_for_mutation "alter_update" "mutation_12.txt"

${CLICKHOUSE_CLIENT} --query="SELECT value2, materialized_value FROM alter_update ORDER BY key"

${CLICKHOUSE_CLIENT} --query="ALTER TABLE alter_update DROP PARTITION 200001"


${CLICKHOUSE_CLIENT} --query="DROP TABLE alter_update"
