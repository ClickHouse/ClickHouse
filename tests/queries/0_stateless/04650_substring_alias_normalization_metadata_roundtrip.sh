#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `substr`/`mid`/`byteSlice` are aliases of `substring` but are parsed by the generic function
# layer, so they accept shapes `SubstringLayer` rejects. DDL normalization renamed them to
# `substring`, and the persisted definition then did not re-parse.
FUNC="f_${CLICKHOUSE_DATABASE}"
VIEWS="v_arity0 v_arity1 v_arity2 v_arity3 v_arity4 v_arity5 v_params v_window v_respect v_ignore v_filter v_all v_mid4 v_byteslice4 v_canonical4 v_sqlstandard"

VIEWS_SQL_LIST=$(for v in $VIEWS; do printf "'%s'," "$v"; done | sed 's/,$//')

drop_views() {
    local sql
    # A view left detached by an attempt that died between DETACH and ATTACH keeps its metadata
    # file, which `DROP VIEW IF EXISTS` skips (it only sees attached objects) and which makes the
    # next `CREATE VIEW` fail with `TABLE_ALREADY_EXISTS ... (detached)`, so reattach those first.
    # `ATTACH TABLE IF NOT EXISTS` cannot be used unconditionally: it throws when there is no
    # metadata at all.
    sql=$($CLICKHOUSE_CLIENT -q "
SELECT 'ATTACH TABLE ' || table || ';'
FROM system.detached_tables
WHERE database = currentDatabase() AND table IN (${VIEWS_SQL_LIST})
")
    for v in $VIEWS; do
        sql+="DROP VIEW IF EXISTS $v; "
    done
    $CLICKHOUSE_CLIENT -q "$sql"
}

$CLICKHOUSE_CLIENT -q "SELECT '-- parser: canonical name accepts the comma form with any argument count'"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring(a, 1, 2, 3)');
SELECT formatQuerySingleLine('SELECT substring(a, 1, 2, 3, 4)');
SELECT formatQuerySingleLine('SELECT substr(a, 1, 2, 3)');
SELECT formatQuerySingleLine('SELECT mid(a, 1, 2, 3)');
SELECT formatQuerySingleLine('SELECT byteSlice(a, 1, 2, 3)');
"

$CLICKHOUSE_CLIENT -q "SELECT '-- parser: the metadata text a pre-fix server wrote re-parses'"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine(\$\$ATTACH VIEW _ UUID '00000000-0000-4000-8000-000000000001' (\`x\` String) AS SELECT substring(a, 1, 2, 3) FROM (SELECT 'q' AS a)\$\$);
"

$CLICKHOUSE_CLIENT -q "SELECT '-- parser: formatting is idempotent'"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT substring(a, 1, 2, 3)')) = formatQuerySingleLine('SELECT substring(a, 1, 2, 3)');
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT substr(a, 1, 2, 3)')) = formatQuerySingleLine('SELECT substr(a, 1, 2, 3)');
"

# The last two rows are three-argument mixed forms that the canonical name already accepted
# before this change; they are pinned here so that tightening them cannot land silently.
$CLICKHOUSE_CLIENT -q "SELECT '-- parser: the SQL standard form and the alias spelling are untouched'"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring(a FROM 2 FOR 3)');
SELECT formatQuerySingleLine('SELECT substring(a FROM 2)');
SELECT formatQuerySingleLine('SELECT substr(a, 2, 3)');
SELECT formatQuerySingleLine('SELECT mid(a, 2)');
SELECT formatQuerySingleLine('SELECT byteSlice(a, 2)');
SELECT formatQuerySingleLine('SELECT substring(a, 1 FOR 2)');
SELECT formatQuerySingleLine('SELECT substring(a FROM 1, 2)');
"

# A trailing comma, fewer than two arguments, and an extra argument after a form that used the
# SQL-standard FROM or FOR keyword must stay parse errors for the canonical name;
# `02154_parser_backtracking` relies on the too-few-arguments one. Both separator orderings of
# the mixed-plus-extra-argument form are asserted, because the extra-argument branch is gated on
# both separators having been commas. Asserted through a server-side parse because the runner
# aborts a batch on a top-level SYNTAX_ERROR.
$CLICKHOUSE_CLIENT -q "SELECT '-- parser: shapes that must keep failing'"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring(a)'); -- { serverError SYNTAX_ERROR }
"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring()'); -- { serverError SYNTAX_ERROR }
"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring(a, 1,)'); -- { serverError SYNTAX_ERROR }
"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring(a FROM 1 FOR 2, 3)'); -- { serverError SYNTAX_ERROR }
"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring(a, 1 FOR 2, 3)'); -- { serverError SYNTAX_ERROR }
"
$CLICKHOUSE_CLIENT -q "
SELECT formatQuerySingleLine('SELECT substring(a FROM 1, 2, 3)'); -- { serverError SYNTAX_ERROR }
"

$CLICKHOUSE_CLIENT -q "SELECT '-- semantics: all four spellings agree, and a too-long call is a function error'"
$CLICKHOUSE_CLIENT -q "
SELECT substring('abcdef', 2, 3), substr('abcdef', 2, 3), mid('abcdef', 2, 3), byteSlice('abcdef', 2, 3);
SELECT substring('abcdef' FROM 2 FOR 3), substring('abcdef' FROM 2), substring('abcdef', 2);
SELECT substr(ALL 'abcdef', 2), mid(ALL 'abcdef', 2), byteSlice(ALL 'abcdef', 2);
"
$CLICKHOUSE_CLIENT -q "SELECT substring('abcdef', 2, 3, 4); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }"
$CLICKHOUSE_CLIENT -q "SELECT substr('abcdef', 2, 3, 4); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }"
$CLICKHOUSE_CLIENT -q "SELECT substr('abcdef'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }"
$CLICKHOUSE_CLIENT -q "SELECT substr(1)('abcdef', 2); -- { serverError FUNCTION_CANNOT_HAVE_PARAMETERS }"

drop_views
# `create_table_query` is produced by re-parsing the metadata file, so a non-empty value proves
# the persisted definition round-trips. It is empty (not an error) when the parse fails.
$CLICKHOUSE_CLIENT -q "SELECT '-- view metadata: every shape round-trips, and safe shapes still canonicalize'"
$CLICKHOUSE_CLIENT -q "
CREATE VIEW v_arity0 (x String) AS SELECT substr() FROM (SELECT 'abc' AS a);
CREATE VIEW v_arity1 (x String) AS SELECT substr(a) FROM (SELECT 'abc' AS a);
CREATE VIEW v_arity2 (x String) AS SELECT substr(a, 1) FROM (SELECT 'abc' AS a);
CREATE VIEW v_arity3 (x String) AS SELECT substr(a, 1, 2) FROM (SELECT 'abc' AS a);
CREATE VIEW v_arity4 (x String) AS SELECT substr(a, 1, 2, 3) FROM (SELECT 'abc' AS a);
CREATE VIEW v_arity5 (x String) AS SELECT substr(a, 1, 2, 3, 4) FROM (SELECT 'abc' AS a);
CREATE VIEW v_params (x String) AS SELECT substr(1)(a, 2) FROM (SELECT 'abc' AS a);
CREATE VIEW v_window (x String) AS SELECT substr(a, 1) OVER () FROM (SELECT 'abc' AS a);
CREATE VIEW v_respect (x String) AS SELECT substr(a, 1) RESPECT NULLS FROM (SELECT 'abc' AS a);
CREATE VIEW v_ignore (x String) AS SELECT substr(a, 1) IGNORE NULLS FROM (SELECT 'abc' AS a);
CREATE VIEW v_filter (x String) AS SELECT substr(a, 1) FILTER (WHERE 1) FROM (SELECT 'abc' AS a);
CREATE VIEW v_all (x String) AS SELECT substr(ALL a, 1) FROM (SELECT 'abc' AS a);
CREATE VIEW v_mid4 (x String) AS SELECT mid(a, 1, 2, 3) FROM (SELECT 'abc' AS a);
CREATE VIEW v_byteslice4 (x String) AS SELECT byteSlice(a, 1, 2, 3) FROM (SELECT 'abc' AS a);
CREATE VIEW v_canonical4 (x String) AS SELECT substring(a, 1, 2, 3) FROM (SELECT 'abc' AS a);
CREATE VIEW v_sqlstandard (x String) AS SELECT substring(a FROM 1 FOR 2) FROM (SELECT 'abc' AS a);
SELECT name, extract(create_table_query, '(substring|substr|mid|byteSlice|substrIf)\(') AS persisted_name
FROM system.tables WHERE database = currentDatabase() AND name LIKE 'v\_%' ORDER BY name;
"

$CLICKHOUSE_CLIENT -q "SELECT '-- view metadata: ATTACH re-reads the file from disk'"
$CLICKHOUSE_CLIENT -q "
DETACH TABLE v_arity0; ATTACH TABLE v_arity0;
DETACH TABLE v_arity1; ATTACH TABLE v_arity1;
DETACH TABLE v_arity4; ATTACH TABLE v_arity4;
DETACH TABLE v_params; ATTACH TABLE v_params;
DETACH TABLE v_window; ATTACH TABLE v_window;
DETACH TABLE v_respect; ATTACH TABLE v_respect;
DETACH TABLE v_ignore; ATTACH TABLE v_ignore;
DETACH TABLE v_canonical4; ATTACH TABLE v_canonical4;
SELECT 'attach ok', count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'v\_%';
"

# `create_query` in `system.functions` is a formatting of the same normalized AST that is written
# to `user_defined/function_*.sql`, and `formatQuerySingleLine` throws on unparseable text, so a
# row that does not error proves the persisted definition re-parses. Limitation: the
# load-from-disk path, where an unparseable body is silently dropped, needs a fresh process and is
# not asserted here. The reference records `f_default` because `tests/clickhouse-test` rewrites the
# randomized database name to `default` in stdout - the function is created as `f_${CLICKHOUSE_DATABASE}`.
$CLICKHOUSE_CLIENT -q "SELECT '-- SQL UDF body: the persisted definition re-parses'"
$CLICKHOUSE_CLIENT -q "
DROP FUNCTION IF EXISTS ${FUNC};
CREATE FUNCTION ${FUNC} AS (a) -> substr(a, 1, 2, 3);
SELECT count(), formatQuerySingleLine(create_query) FROM system.functions WHERE name = '${FUNC}' GROUP BY create_query;
DROP FUNCTION ${FUNC};
"
$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION ${FUNC} AS (a) -> substr(a);
SELECT count(), formatQuerySingleLine(create_query) FROM system.functions WHERE name = '${FUNC}' GROUP BY create_query;
DROP FUNCTION ${FUNC};
"

drop_views
$CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS ${FUNC}"
