#!/usr/bin/env bash
# `SQL UDF` expansion drops the function's body into the query after the main qualification pass,
# so the table names it brings in are qualified by the narrow pass instead. That pass has to honour
# the same `WITH` scoping as the main one: inside a body that turns `enable_global_with_statement`
# off, a common table expression of the enclosing `SELECT` is not visible, so the name is an
# ordinary table of the database owning the definition and must be qualified. Leaving it bare made
# the stored view read the table of whichever database queried it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `SQL UDF` is server-global, so every name carries the database to stay parallel-safe.
F_OUT="f_out_of_scope_${CLICKHOUSE_DATABASE}"
F_IN="f_in_scope_${CLICKHOUSE_DATABASE}"
F_OWN="f_own_body_${CLICKHOUSE_DATABASE}"
F_SEED="f_recursive_seed_${CLICKHOUSE_DATABASE}"
F_NESTED="f_nested_same_name_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
CREATE DATABASE ${CLICKHOUSE_DATABASE_1};

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (11);
CREATE TABLE ${CLICKHOUSE_DATABASE_1}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE_1}.src VALUES (22);

CREATE FUNCTION ${F_OUT} AS () -> (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0);
CREATE FUNCTION ${F_IN} AS () -> (SELECT max(id) FROM src);

-- The live answers, which the stored ones have to match: the body that disables inheritance reads
-- the table, the one that inherits reads the common table expression.
SELECT 'live out of scope', (WITH src AS (SELECT 7 AS id) SELECT ${F_OUT}());
SELECT 'live in scope', (WITH src AS (SELECT 7 AS id) SELECT ${F_IN}());

CREATE VIEW v_out_of_scope AS WITH src AS (SELECT 7 AS id) SELECT ${F_OUT}() AS x;
CREATE VIEW v_in_scope AS WITH src AS (SELECT 7 AS id) SELECT ${F_IN}() AS x;

-- The name the enclosing \`WITH\` does not reach is qualified; the one it reaches is left alone.
SELECT 'stored out of scope qualified', position(create_table_query, currentDatabase() || '.src') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_out_of_scope';
SELECT 'stored in scope not qualified', position(create_table_query, currentDatabase() || '.src') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_in_scope';

-- Read from another database that holds a table of the same name: the answers must not move.
USE ${CLICKHOUSE_DATABASE_1};
SELECT 'read out of scope', x FROM ${CLICKHOUSE_DATABASE}.v_out_of_scope;
SELECT 'read in scope', x FROM ${CLICKHOUSE_DATABASE}.v_in_scope;
USE ${CLICKHOUSE_DATABASE};

-- Inside a \`WITH\` element's own body the name is not the element: a plain element, and the seed
-- of a recursive one, read the table, so those names have to be qualified as well - the same rule
-- the main pass applies through \`BodyWalk\`. The recursive members after the seed do reference the
-- element and must stay bare, and a same-named element of an enclosing \`SELECT\` still hides the
-- table, so only the innermost declaration is masked.
CREATE FUNCTION ${F_OWN} AS () -> (WITH src AS (SELECT max(id) AS m FROM src) SELECT m FROM src);
CREATE FUNCTION ${F_SEED} AS () ->
    (WITH RECURSIVE src AS (SELECT max(id) AS m FROM src UNION ALL SELECT m FROM src WHERE 0) SELECT max(m) FROM src);
CREATE FUNCTION ${F_NESTED} AS () ->
    (WITH src AS (SELECT 5 AS m) SELECT (WITH src AS (SELECT max(m) * 2 AS m FROM src) SELECT m FROM src));

SELECT 'live own body', ${F_OWN}();
SELECT 'live recursive seed', ${F_SEED}();
SELECT 'live nested same name', ${F_NESTED}();

CREATE VIEW v_own_body AS SELECT ${F_OWN}() AS x;
CREATE VIEW v_recursive_seed AS SELECT ${F_SEED}() AS x;
CREATE VIEW v_nested_same_name AS SELECT ${F_NESTED}() AS x;

SELECT 'stored own body qualified', position(create_table_query, currentDatabase() || '.src') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_own_body';
SELECT 'stored recursive seed qualified', position(create_table_query, currentDatabase() || '.src') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_recursive_seed';
-- No table is involved in the nested shape: both names are common table expressions there.
SELECT 'stored nested same name not qualified', position(create_table_query, currentDatabase() || '.src') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_nested_same_name';

USE ${CLICKHOUSE_DATABASE_1};
SELECT 'read own body', x FROM ${CLICKHOUSE_DATABASE}.v_own_body;
SELECT 'read recursive seed', x FROM ${CLICKHOUSE_DATABASE}.v_recursive_seed;
SELECT 'read nested same name', x FROM ${CLICKHOUSE_DATABASE}.v_nested_same_name;
USE ${CLICKHOUSE_DATABASE};

-- The stored text is what a reload re-derives the answer from.
DETACH TABLE v_out_of_scope;
ATTACH TABLE v_out_of_scope;
SELECT 'reload out of scope', x FROM v_out_of_scope;

DROP FUNCTION ${F_OUT};
DROP FUNCTION ${F_IN};
DROP FUNCTION ${F_OWN};
DROP FUNCTION ${F_SEED};
DROP FUNCTION ${F_NESTED};
DROP DATABASE ${CLICKHOUSE_DATABASE_1};
EOF
