#!/usr/bin/env bash
# https://github.com/ClickHouse/ClickHouse/issues/82084
#
# A materialized CTE can have the same name as a database. A failed lookup behind the CTE qualifier
# must fall through, so that the database-qualified interpretation of the same first identifier part
# (`db.table.column` referring to a real table in that database) is still attempted.
#
# This scenario lives in a shell test rather than in `04612_table_same_name_as_database.sql` because
# a CTE name cannot be a query parameter (`{name:Identifier}`) - the parser rejects it - so the
# database name has to be substituted into the query text directly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE ${DB}.other (id Int32, value Int32) ENGINE = MergeTree ORDER BY ();
INSERT INTO ${DB}.other VALUES (42, 1);
"

# The first identifier part matches the materialized CTE name; the lookup behind the CTE fails and
# must fall through to the database-qualified interpretation (`${DB}.other.value`).
$CLICKHOUSE_CLIENT --query "
WITH ${DB} AS MATERIALIZED (SELECT 42 AS id)
SELECT ${DB}.other.value
FROM ${DB}
JOIN ${DB}.other USING (id)
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1;
"
