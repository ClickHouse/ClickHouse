#!/usr/bin/env bash
# `joinGet` takes a table name in its first argument, and `ApplyWithSubqueryVisitor` leaves that
# argument alone where it rewrites the right side of `IN`, so a `WITH` expression alias used there
# still reaches the narrow qualification pass as an identifier. The pass has to decide it the way
# the analyzer does: an alias of an enclosing `SELECT` is not a table name, and a body that stops
# inheriting sees the table again. Qualifying an alias made the stored definition read a `Join`
# table of the database owning it rather than the one the alias names, and leaving a name that is
# not an alias bare made the definition follow whoever queries it.
#
# The alias that wins names its table by string, and `joinGet` resolves a string against the
# current database of whoever reads it, so that answer moves with the reader in the live query and
# in the stored definition alike. The cross-database arms assert that agreement: it is the reason
# the identifier, and not the value it resolves to, is the part qualification has to pin.
#
# `enable_scopes_for_with_statement` is covered here on the reading body, where turning it off does
# not make an alias visible that `enable_global_with_statement = 0` had already hidden, because the
# alias set the analyzer keeps for disabled scopes is filled by a `SELECT` that itself disables
# them.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `SQL UDF` is server-global, so every name carries the database to stay parallel-safe.
F_IN="f_join_get_in_scope_${CLICKHOUSE_DATABASE}"
F_OUT="f_join_get_out_of_scope_${CLICKHOUSE_DATABASE}"
F_OUT_NO_SCOPES="f_join_get_out_of_scope_no_scopes_${CLICKHOUSE_DATABASE}"
F_NO_SCOPES="f_join_get_no_scopes_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
CREATE DATABASE ${CLICKHOUSE_DATABASE_1};

-- 7 = the alias won and \`joinGet\` read the table it names, 1 = the name was taken for a table.
CREATE TABLE join_source (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join_source VALUES (1, 7);
CREATE TABLE src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO src VALUES (1, 1);
-- The same two names elsewhere, holding different values: a definition which is not pinned to the
-- database owning it is then caught by the answer moving, and one which is pinned by it staying.
CREATE TABLE ${CLICKHOUSE_DATABASE_1}.src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO ${CLICKHOUSE_DATABASE_1}.src VALUES (1, 99);
CREATE TABLE ${CLICKHOUSE_DATABASE_1}.join_source (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO ${CLICKHOUSE_DATABASE_1}.join_source VALUES (1, 77);

CREATE FUNCTION ${F_IN} AS (k) -> (SELECT joinGet(src, 'v', k));
CREATE FUNCTION ${F_OUT} AS (k) -> (SELECT joinGet(src, 'v', k) SETTINGS enable_global_with_statement = 0);
CREATE FUNCTION ${F_OUT_NO_SCOPES} AS (k) ->
    (SELECT joinGet(src, 'v', k) SETTINGS enable_global_with_statement = 0, enable_scopes_for_with_statement = 0);
CREATE FUNCTION ${F_NO_SCOPES} AS (k) -> (SELECT joinGet(src, 'v', k) SETTINGS enable_scopes_for_with_statement = 0);

-- The live answers, which the stored ones have to match.
SELECT 'live in scope', (WITH 'join_source' AS src SELECT ${F_IN}(toUInt64(1)));
SELECT 'live out of scope', (WITH 'join_source' AS src SELECT ${F_OUT}(toUInt64(1)));
SELECT 'live out of scope without scopes', (WITH 'join_source' AS src SELECT ${F_OUT_NO_SCOPES}(toUInt64(1)));
SELECT 'live without scopes', (WITH 'join_source' AS src SELECT ${F_NO_SCOPES}(toUInt64(1)));

CREATE VIEW v_in_scope AS WITH 'join_source' AS src SELECT ${F_IN}(toUInt64(1)) AS r;
CREATE VIEW v_out_of_scope AS WITH 'join_source' AS src SELECT ${F_OUT}(toUInt64(1)) AS r;
CREATE VIEW v_out_of_scope_no_scopes AS WITH 'join_source' AS src SELECT ${F_OUT_NO_SCOPES}(toUInt64(1)) AS r;
CREATE VIEW v_no_scopes AS WITH 'join_source' AS src SELECT ${F_NO_SCOPES}(toUInt64(1)) AS r;

SELECT 'view in scope', r FROM v_in_scope;
SELECT 'view out of scope', r FROM v_out_of_scope;
SELECT 'view out of scope without scopes', r FROM v_out_of_scope_no_scopes;
SELECT 'view without scopes', r FROM v_no_scopes;

-- The name the enclosing \`WITH\` reaches is left bare, the one it does not reach is qualified.
SELECT 'stored in scope is bare', position(create_table_query, currentDatabase() || '.src') = 0 AND position(create_table_query, '.\`src\`') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_in_scope';
SELECT 'stored out of scope is qualified', position(create_table_query, currentDatabase() || '.src') > 0 OR position(create_table_query, '.\`src\`') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_out_of_scope';
SELECT 'stored out of scope without scopes is qualified', position(create_table_query, currentDatabase() || '.src') > 0 OR position(create_table_query, '.\`src\`') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_out_of_scope_no_scopes';
SELECT 'stored without scopes is bare', position(create_table_query, currentDatabase() || '.src') = 0 AND position(create_table_query, '.\`src\`') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_no_scopes';

-- From a database holding both names: a qualified name keeps reading the owner's table, and the
-- alias resolves against the reader exactly as the live query does.
USE ${CLICKHOUSE_DATABASE_1};
SELECT 'read out of scope elsewhere', r FROM ${CLICKHOUSE_DATABASE}.v_out_of_scope;
SELECT 'read out of scope without scopes elsewhere', r FROM ${CLICKHOUSE_DATABASE}.v_out_of_scope_no_scopes;
SELECT 'read in scope elsewhere', r FROM ${CLICKHOUSE_DATABASE}.v_in_scope;
SELECT 'live in scope elsewhere', (WITH 'join_source' AS src SELECT ${F_IN}(toUInt64(1)));
USE ${CLICKHOUSE_DATABASE};

-- The stored text is what a reload re-derives the answers from.
DETACH TABLE v_in_scope;
ATTACH TABLE v_in_scope;
DETACH TABLE v_out_of_scope_no_scopes;
ATTACH TABLE v_out_of_scope_no_scopes;
DETACH TABLE v_no_scopes;
ATTACH TABLE v_no_scopes;
SELECT 'reload in scope', r FROM v_in_scope;
SELECT 'reload out of scope without scopes', r FROM v_out_of_scope_no_scopes;
SELECT 'reload without scopes', r FROM v_no_scopes;

DROP FUNCTION ${F_IN};
DROP FUNCTION ${F_OUT};
DROP FUNCTION ${F_OUT_NO_SCOPES};
DROP FUNCTION ${F_NO_SCOPES};
DROP DATABASE ${CLICKHOUSE_DATABASE_1};
EOF
