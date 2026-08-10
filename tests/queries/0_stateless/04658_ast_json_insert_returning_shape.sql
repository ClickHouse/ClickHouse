-- `ASTInsertQuery` stores delayed `RETURNING` and source-side trailing `SETTINGS` in
-- dedicated fields. AST JSON must carry them as well, otherwise JSON round-trip silently
-- degrades `INSERT ... RETURNING ... SETTINGS ...` into plain `INSERT ... SELECT`.

SELECT formatQueryFromJSON(parseQueryToJSON(
    $$INSERT INTO t SELECT number FROM numbers(3) RETURNING (SELECT count()) SETTINGS max_threads = 1$$))
    = formatQuerySingleLine(
        $$INSERT INTO t SELECT number FROM numbers(3) RETURNING (SELECT count()) SETTINGS max_threads = 1$$);

SELECT position(
    parseQueryToJSON($$INSERT INTO t SELECT number FROM numbers(3) RETURNING (SELECT count()) SETTINGS max_threads = 1$$),
    '"returning_select":{"type":"SelectWithUnionQuery"') > 0;

SELECT position(
    parseQueryToJSON($$INSERT INTO t SELECT number FROM numbers(3) RETURNING (SELECT count()) SETTINGS max_threads = 1$$),
    '"source_select_settings_ast":{"type":"SetQuery"') > 0;

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$INSERT INTO t SELECT number FROM numbers(3) RETURNING (SELECT count()) SETTINGS max_threads = 1$$),
    '"returning_select":{"type":"SelectWithUnionQuery"',
    '"returning_select":{"type":"Literal","value":{"field_type":"UInt64","value":1}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON($$INSERT INTO t SELECT number FROM numbers(3) RETURNING (SELECT count()) SETTINGS max_threads = 1$$),
    '"source_select_settings_ast":{"type":"SetQuery"',
    '"source_select_settings_ast":{"type":"Literal","value":{"field_type":"UInt64","value":1}')); -- { serverError BAD_ARGUMENTS }
