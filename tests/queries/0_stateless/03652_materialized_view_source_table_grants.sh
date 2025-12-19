#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_NAME="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

##############################################################################
# Test 1: MV column that doesn't exist in source table (original issue)
# MV has column "number" that doesn't exist in source_table
##############################################################################
echo "=== Test 1: MV column not in source table ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS source_table;
DROP TABLE IF EXISTS mv_table;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE source_table (date Date) ENGINE = Null;

CREATE MATERIALIZED VIEW mv_table (date Date, number UInt32)
ENGINE = MergeTree PARTITION BY date ORDER BY (date, number)
AS SELECT date FROM source_table;

INSERT INTO source_table VALUES ('2000-01-01');

CREATE USER ${USER_NAME};
GRANT SELECT(date) ON source_table TO ${USER_NAME};
GRANT SELECT ON mv_table TO ${USER_NAME};
"

# Should succeed - user has SELECT(date) on source and SELECT on mv
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT date, number FROM mv_table;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv_table;
DROP TABLE source_table;
"

##############################################################################
# Test 2: Aliased columns
# SELECT secret AS public - need access to "secret", not "public"
##############################################################################
echo "=== Test 2: Aliased columns ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv1;
DROP TABLE IF EXISTS mv2;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (public String, secret String) ENGINE = Null;

-- mv1: SELECT secret (no alias) - MV column is 'secret'
CREATE MATERIALIZED VIEW mv1 (secret String)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT secret FROM src;

-- mv2: SELECT secret AS public - MV column is 'public' but needs source column 'secret'
CREATE MATERIALIZED VIEW mv2 (public String)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT secret AS public FROM src;

INSERT INTO src VALUES ('pub_val', 'secret_val');

CREATE USER ${USER_NAME};
GRANT SELECT(public) ON src TO ${USER_NAME};
GRANT SELECT ON mv1 TO ${USER_NAME};
GRANT SELECT ON mv2 TO ${USER_NAME};
"

# mv1: Should FAIL - user has SELECT(public) but mv1 needs SELECT(secret)
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT * FROM mv1;" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "OK"

# mv2: Should FAIL - user has SELECT(public) but mv2 also needs SELECT(secret) due to alias
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT * FROM mv2;" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "OK"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv2;
DROP TABLE mv1;
DROP TABLE src;
"

##############################################################################
# Test 3: Proper grants with aliases
##############################################################################
echo "=== Test 3: Proper grants with aliases ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (a Int, b Int, c Int) ENGINE = Null;

-- MV with alias: b AS x
CREATE MATERIALIZED VIEW mv (x Int)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT b AS x FROM src;

INSERT INTO src VALUES (1, 2, 3);

CREATE USER ${USER_NAME};
GRANT SELECT(b) ON src TO ${USER_NAME};
GRANT SELECT ON mv TO ${USER_NAME};
"

# Should succeed - user has SELECT(b) which is what the MV needs
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT x FROM mv;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv;
DROP TABLE src;
"

##############################################################################
# Test 4: Computed columns (expressions)
##############################################################################
echo "=== Test 4: Computed columns ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (a Int, b Int) ENGINE = Null;

-- MV with expression: a + b AS sum
CREATE MATERIALIZED VIEW mv (sum Int)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT a + b AS sum FROM src;

INSERT INTO src VALUES (1, 2);

CREATE USER ${USER_NAME};
GRANT SELECT(a) ON src TO ${USER_NAME};
GRANT SELECT ON mv TO ${USER_NAME};
"

# Should FAIL - user has SELECT(a) but mv needs SELECT(a, b)
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT sum FROM mv;" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "OK"

# Now grant SELECT(b) as well
$CLICKHOUSE_CLIENT --query "GRANT SELECT(b) ON src TO ${USER_NAME};"

# Should succeed now
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT sum FROM mv;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv;
DROP TABLE src;
"

##############################################################################
# Test 5: Partial column access - only query some MV columns
##############################################################################
echo "=== Test 5: Partial column access ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (a Int, b Int, c Int) ENGINE = Null;

-- MV selects all three columns
CREATE MATERIALIZED VIEW mv (a Int, b Int, c Int)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT a, b, c FROM src;

INSERT INTO src VALUES (1, 2, 3);

CREATE USER ${USER_NAME};
GRANT SELECT(a) ON src TO ${USER_NAME};
GRANT SELECT ON mv TO ${USER_NAME};
"

# Query only column 'a' - should succeed with just SELECT(a) on source
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT a FROM mv;"

# Query column 'b' - should FAIL, user doesn't have SELECT(b) on source
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT b FROM mv;" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "OK"

# Query all columns - should FAIL
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT * FROM mv;" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "OK"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv;
DROP TABLE src;
"

##############################################################################
# Test 6: UNION queries - need access to columns from all UNION parts
##############################################################################
echo "=== Test 6: UNION queries ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (a Int, b Int, c Int) ENGINE = Null;

-- MV with UNION: first part uses 'a', second part uses 'b'
CREATE MATERIALIZED VIEW mv (x Int)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT a AS x FROM src UNION ALL SELECT b AS x FROM src;

INSERT INTO src VALUES (1, 2, 3);

CREATE USER ${USER_NAME};
GRANT SELECT(a) ON src TO ${USER_NAME};
GRANT SELECT ON mv TO ${USER_NAME};
"

# Should FAIL - user has SELECT(a) but MV also needs SELECT(b) from second UNION part
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT x FROM mv;" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "OK"

# Grant SELECT(b) as well
$CLICKHOUSE_CLIENT --query "GRANT SELECT(b) ON src TO ${USER_NAME};"

# Should succeed now - user has both SELECT(a) and SELECT(b)
# NOTE: We check access for all unioned select parts, but the MV only inserts from the first part. So the expected result is 1 here.
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT x FROM mv;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv;
DROP TABLE src;
"

##############################################################################
# Test 7: JOIN queries - only source table (left) columns are checked
##############################################################################
echo "=== Test 7: JOIN queries ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS lookup;
DROP TABLE IF EXISTS mv;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (id Int, value Int) ENGINE = Null;
CREATE TABLE lookup (id Int, name String) ENGINE = Memory;
INSERT INTO lookup VALUES (1, 'one'), (2, 'two');

-- MV with JOIN: src is the source table, lookup is the joined table
CREATE MATERIALIZED VIEW mv (id Int, value Int, name String)
ENGINE = MergeTree ORDER BY tuple()
AS SELECT src.id, src.value, lookup.name FROM src JOIN lookup ON src.id = lookup.id;

INSERT INTO src VALUES (1, 100);

CREATE USER ${USER_NAME};
GRANT SELECT(id, value) ON src TO ${USER_NAME};
GRANT SELECT ON mv TO ${USER_NAME};
-- NOTE: No grants on lookup table
"

# Should succeed - user has grants on source table columns, lookup table is not checked
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT * FROM mv;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv;
DROP TABLE lookup;
DROP TABLE src;
"

##############################################################################
# Test 8: CTE queries - CTEs are inlined, only source table columns checked
##############################################################################
echo "=== Test 8: CTE queries ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS other_table;
DROP TABLE IF EXISTS mv;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (id Int, value Int) ENGINE = Null;
CREATE TABLE other_table (id Int, extra String) ENGINE = Memory;
INSERT INTO other_table VALUES (1, 'extra_data');

-- MV with CTE that references another table
CREATE MATERIALIZED VIEW mv (id Int, value Int, extra String)
ENGINE = MergeTree ORDER BY tuple()
AS WITH cte AS (SELECT id, extra FROM other_table)
SELECT src.id, src.value, cte.extra FROM src JOIN cte ON src.id = cte.id;

INSERT INTO src VALUES (1, 100);

CREATE USER ${USER_NAME};
GRANT SELECT(id, value) ON src TO ${USER_NAME};
GRANT SELECT ON mv TO ${USER_NAME};
-- NOTE: No grants on other_table
"

# Should succeed - CTEs are inlined, only source table columns are checked
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT * FROM mv;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv;
DROP TABLE other_table;
DROP TABLE src;
"

##############################################################################
# Test 9: CTE from same source table accessed via JOIN
# NOTE: When a CTE is accessed via JOIN, it's treated like a joined table,
# so columns from the CTE are not checked against source table grants.
##############################################################################
echo "=== Test 9: CTE from same source table ==="

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
DROP USER IF EXISTS ${USER_NAME};

CREATE TABLE src (id Int, value Int, category String) ENGINE = Null;

-- MV with CTE that references the same source table but different column
CREATE MATERIALIZED VIEW mv (id Int, value Int, cat String)
ENGINE = MergeTree ORDER BY tuple()
AS WITH categories AS (SELECT id, category FROM src)
SELECT src.id, src.value, categories.category AS cat FROM src JOIN categories ON src.id = categories.id;

INSERT INTO src VALUES (1, 100, 'A');

CREATE USER ${USER_NAME};
GRANT SELECT(id, value) ON src TO ${USER_NAME};
GRANT SELECT ON mv TO ${USER_NAME};
-- NOTE: No grant on 'category' column, but CTE is accessed via JOIN so it's not checked
"

# Should succeed - CTE accessed via JOIN is treated like a joined table
$CLICKHOUSE_CLIENT --user=${USER_NAME} --query "SELECT * FROM mv;"

$CLICKHOUSE_CLIENT -n --query "
DROP USER ${USER_NAME};
DROP TABLE mv;
DROP TABLE src;
"
