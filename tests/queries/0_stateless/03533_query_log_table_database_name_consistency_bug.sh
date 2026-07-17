#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We need the predefined log_comment to identify log entities during the test run only.
uuid=$(${CLICKHOUSE_CLIENT} --query "SELECT reinterpretAsUUID(currentDatabase())")
log_comment="03533_table_names_consistency_in_query_log_table_bug_$uuid"

# The test checks that the system.query_log contains consistency table and database names
# for queries involving tables and databases with and without dashes in their names.

# Clean up any previous runs
$CLICKHOUSE_CLIENT -q "
-- Clean up before running the test
DROP TABLE IF EXISTS \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`;
DROP TABLE IF EXISTS test_without_dash;
DROP TABLE IF EXISTS \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash-2\`;
DROP TABLE IF EXISTS test_without_dash_2;
DROP DATABASE IF EXISTS \`03533-query-log-table-database-name-consistency-bug\`;
"

# Data preparation for the test.
$CLICKHOUSE_CLIENT -q "
SET log_queries=1;

-- Create the database and tables with and without dashes in their names
CREATE DATABASE \`03533-query-log-table-database-name-consistency-bug\`
SETTINGS log_comment='$log_comment';

CREATE TABLE \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\` (id Int32)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192, log_comment='$log_comment';

CREATE TABLE test_without_dash (id Int32)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192, log_comment='$log_comment';

-- Insertion data queries
INSERT INTO \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`
SETTINGS log_comment='$log_comment', async_insert=1, wait_for_async_insert=1
VALUES (1);

INSERT INTO test_without_dash
SETTINGS log_comment='$log_comment', async_insert=1, wait_for_async_insert=1
VALUES (1);

-- Selection data queries
SELECT * FROM \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`
SETTINGS log_comment='$log_comment';

SELECT * FROM test_without_dash
SETTINGS log_comment='$log_comment';

SELECT * FROM \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`
UNION ALL
SELECT * FROM test_without_dash
SETTINGS log_comment='$log_comment';

SELECT * FROM \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\` AS a
LEFT JOIN test_without_dash AS b USING(id)
SETTINGS log_comment='$log_comment';

-- Alteration queries
ALTER TABLE \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`
ADD COLUMN new_column Int32
SETTINGS log_comment='$log_comment';

ALTER TABLE test_without_dash ADD COLUMN new_column Int32
SETTINGS log_comment='$log_comment';

-- Renaming tables/databases queries
RENAME TABLE \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`
TO \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash-2\`
SETTINGS log_comment='$log_comment';

RENAME TABLE \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash-2\`
TO \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`
SETTINGS log_comment='$log_comment';

RENAME TABLE test_without_dash TO test_without_dash_2
SETTINGS log_comment='$log_comment';

RENAME TABLE test_without_dash_2 TO test_without_dash
SETTINGS log_comment='$log_comment';

-- Dropping tables/databases queries
DROP TABLE \`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`
SETTINGS log_comment='$log_comment';

DROP TABLE test_without_dash
SETTINGS log_comment='$log_comment';

DROP DATABASE \`03533-query-log-table-database-name-consistency-bug\`
SETTINGS log_comment='$log_comment';
"

# Flush the logs and check the system.query_log for consistency in table and database names.
$CLICKHOUSE_CLIENT -q "
SYSTEM FLUSH LOGS query_log;

WITH
    [
        currentDatabase(),
        '\`03533-query-log-table-database-name-consistency-bug\`'
    ] AS allowed_databases,
    [
        '\`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash\`',
        '\`03533-query-log-table-database-name-consistency-bug\`.\`test-with-a-dash-2\`',
        concat(currentDatabase(), '.test_without_dash'),
        concat(currentDatabase(), '.test_without_dash_2')
    ] AS allowed_tables
SELECT
    countIf(
        arrayExists(t -> t NOT IN allowed_tables, tables) OR
        arrayExists(d -> d NOT IN allowed_databases, databases)
    ) AS unexpected_entries_found
FROM system.query_log
WHERE
    log_comment = '$log_comment' AND
    (
        current_database = currentDatabase() OR
        hasAny(databases, [currentDatabase(), '\`03533-query-log-table-database-name-consistency-bug\`'])
    );
"
