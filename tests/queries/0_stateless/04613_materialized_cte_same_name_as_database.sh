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

# Under `analyzer_compatibility_prefer_alias_over_subcolumn = 1` the materialized CTE name is a
# qualifier carrier like an alias or a table name: when the lookup behind it succeeds, resolution is
# pruned to the CTE side of the JOIN instead of competing with the database-qualified interpretation
# (`${DB}.tbl.value`) of the other side, which would throw `AMBIGUOUS_IDENTIFIER`.
$CLICKHOUSE_CLIENT --query "
CREATE TABLE ${DB}.tbl (id Int32, value Int32) ENGINE = MergeTree ORDER BY ();
INSERT INTO ${DB}.tbl VALUES (42, 7);
"

$CLICKHOUSE_CLIENT --query "
WITH ${DB} AS MATERIALIZED (SELECT 42 AS id, 99 AS \`tbl.value\`)
SELECT ${DB}.tbl.value
FROM ${DB}
JOIN ${DB}.tbl USING (id)
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1,
    analyzer_compatibility_prefer_alias_over_subcolumn = 1, single_join_prefer_left_table = 0;
"

# The same compatibility path also has to treat the visible CTE name as a qualifier carrier when the
# qualified column competes with a same-named subcolumn: `cte.id` must resolve to the qualified column
# `id` of the CTE, not to the `id` element of the CTE column `cte`, exactly as it does for a regular CTE
# or a subquery alias. A materialized CTE is stored under an internal temporary table name, so without
# the CTE name this path is never taken for it.
$CLICKHOUSE_CLIENT --query "
WITH cte AS MATERIALIZED (SELECT 1 AS id, CAST(tuple(2), 'Tuple(id Int32)') AS cte)
SELECT cte.id
FROM cte
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1,
    analyzer_compatibility_prefer_alias_over_subcolumn = 1;
"

# The same shape with a regular CTE, which is the behavior the materialized CTE has to match.
$CLICKHOUSE_CLIENT --query "
WITH cte AS (SELECT 1 AS id, CAST(tuple(2), 'Tuple(id Int32)') AS cte)
SELECT cte.id
FROM cte
SETTINGS enable_analyzer = 1, analyzer_compatibility_prefer_alias_over_subcolumn = 1;
"

# The qualifier of a qualified matcher is looked up as a table expression after the expression lookup
# misses, and a materialized CTE is stored under an internal temporary table name. The CTE name has to
# be accepted there as well, otherwise `${DB}.*` throws `Qualified matcher does not find table`.
$CLICKHOUSE_CLIENT --query "
WITH ${DB} AS MATERIALIZED (SELECT 42 AS id, 7 AS v)
SELECT ${DB}.*
FROM ${DB}
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1;
"

# The same matcher shape with a CTE name that does not collide with any database name.
$CLICKHOUSE_CLIENT --query "
WITH cte AS MATERIALIZED (SELECT 42 AS id)
SELECT cte.*
FROM cte
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1;
"

# When a matcher-expanded column has to be qualified (another table expression in scope binds the same
# column name), the qualification must use the visible CTE name. A materialized CTE lives under a
# randomly generated internal temporary table name, which would otherwise leak into the result header
# and make the schema unstable across runs.
$CLICKHOUSE_CLIENT --query "
WITH cte AS MATERIALIZED (SELECT 1 AS id)
SELECT t.*, cte.*
FROM (SELECT 2 AS id) AS t, cte
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1
FORMAT TSVWithNames;
"

# An explicit alias still takes precedence over the CTE name.
$CLICKHOUSE_CLIENT --query "
WITH cte AS MATERIALIZED (SELECT 1 AS id)
SELECT t.*, c2.*
FROM (SELECT 2 AS id) AS t, cte AS c2
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1
FORMAT TSVWithNames;
"

# The CTE name is visible for the generic binder too: a matcher-expanded column of another table
# expression whose name is already qualified with the table name (`tbl.id`) still binds to a
# materialized CTE named `tbl`, so it has to be qualified with the database name as well.
$CLICKHOUSE_CLIENT --query "
WITH tbl AS MATERIALIZED (SELECT 2 AS id)
SELECT *
FROM tbl, ${DB}.tbl
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1
FORMAT TSVWithNames;
" | sed "s/${DB}/db/g"

$CLICKHOUSE_CLIENT --query "
WITH tbl AS MATERIALIZED (SELECT 2 AS id)
SELECT ${DB}.tbl.*
FROM tbl, ${DB}.tbl
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1
FORMAT TSVWithNames;
" | sed "s/${DB}/db/g"

# A qualifier matching both a materialized CTE and a table is ambiguous, exactly as it is for a
# regular CTE.
$CLICKHOUSE_CLIENT --query "
WITH tbl AS MATERIALIZED (SELECT 2 AS id)
SELECT ${DB}.tbl.*, tbl.*
FROM ${DB}.tbl, tbl
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1;
" 2>&1 | grep -o -m1 'AMBIGUOUS_IDENTIFIER'

$CLICKHOUSE_CLIENT --query "
WITH tbl AS (SELECT 2 AS id)
SELECT ${DB}.tbl.*, tbl.*
FROM ${DB}.tbl, tbl
SETTINGS enable_analyzer = 1;
" 2>&1 | grep -o -m1 'AMBIGUOUS_IDENTIFIER'

# The forced qualification of the multiple-joins compatibility mode
# (analyzer_compatibility_multiple_joins_qualify_column_names, two or more JOINs) must also use the
# visible CTE name for a materialized CTE, not the internal temporary table name it is stored under.
$CLICKHOUSE_CLIENT --query "
WITH cte AS MATERIALIZED (SELECT 1 AS k, 'D' AS Date)
SELECT cte.Date
FROM
(
    SELECT *
    FROM cte
    LEFT JOIN (SELECT 1 AS k) AS t1 ON cte.k = t1.k
    LEFT JOIN (SELECT 1 AS k) AS t2 ON cte.k = t2.k
)
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1,
    analyzer_compatibility_multiple_joins_qualify_column_names = 1;
"

# The same shape with a regular CTE, which is the behavior the materialized CTE has to match.
$CLICKHOUSE_CLIENT --query "
WITH cte AS (SELECT 1 AS k, 'D' AS Date)
SELECT cte.Date
FROM
(
    SELECT *
    FROM cte
    LEFT JOIN (SELECT 1 AS k) AS t1 ON cte.k = t1.k
    LEFT JOIN (SELECT 1 AS k) AS t2 ON cte.k = t2.k
)
SETTINGS enable_analyzer = 1,
    analyzer_compatibility_multiple_joins_qualify_column_names = 1;
"

# The forced qualifier is directly visible in the header of the matcher expansion.
$CLICKHOUSE_CLIENT --query "
WITH cte AS MATERIALIZED (SELECT 1 AS k)
SELECT cte.*
FROM cte
LEFT JOIN (SELECT 1 AS k) AS t1 ON cte.k = t1.k
LEFT JOIN (SELECT 1 AS k) AS t2 ON cte.k = t2.k
SETTINGS enable_materialized_cte = 1, enable_analyzer = 1,
    analyzer_compatibility_multiple_joins_qualify_column_names = 1
FORMAT TSVWithNames;
"
