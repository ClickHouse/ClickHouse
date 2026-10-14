-- Identifiers that the parser reads back as a literal rather than an identifier must be quoted when
-- an AST is formatted. `ParserNumber` hands a bare word to `strtod`, which accepts `inf`, `infinity`
-- and `nan`, and `true`/`false` are parsed as `Bool` literals, so leaving such a name unquoted
-- silently turns a column reference into a constant.

SELECT formatQuerySingleLine('SELECT `inf`, `infinity`, `nan`, `true`, `false`');

-- The parser accepts these names in any case, so all of them have to be quoted.
SELECT formatQuerySingleLine('SELECT `Inf`, `INF`, `Infinity`, `NaN`, `NAN`, `True`, `TRUE`, `False`, `FALSE`');

-- `null` is quoted as well, and a name that merely starts like one of them is not.
SELECT formatQuerySingleLine('SELECT `null`, `NULL`, `infx`, `nanx`, `inf_1`');

-- The sorting key of a table with such a column is written to its metadata in formatted form.
-- Unquoted it was read back as a constant, and the table could no longer be attached:
-- `Sorting key cannot contain constants`.
DROP TABLE IF EXISTS t_literal_names;
CREATE TABLE t_literal_names (`inf` Int32, `nan` Float64, `true` Int32, `x` Int32)
ENGINE = MergeTree ORDER BY (`inf`, `nan`, `true`);

SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_literal_names';

INSERT INTO t_literal_names VALUES (1, 2, 3, 4);

-- `ATTACH` re-reads the metadata file, which is what fails on the next server start.
DETACH TABLE t_literal_names;
ATTACH TABLE t_literal_names;

SELECT count() FROM t_literal_names;

DROP TABLE t_literal_names;
