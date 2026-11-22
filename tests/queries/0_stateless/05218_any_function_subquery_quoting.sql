-- `ANY` on the right of a comparison is the SQL quantifier syntax, which the parser rewrites to `IN` at parse
-- time. `any` is also an aggregate function, so a function of that name holding a single subquery argument has
-- to be formatted quoted, or the text reads back as the quantifier and the comparison silently becomes `IN`.
-- The formatter cases below only format: the consistency check in `executeQuery` compares a parsed AST with a
-- re-parse of its own output, so a regressed formatter would abort a debug server on the offending shape
-- instead of reddening one line. The alias case at the end is executed because only the interpreter produces it.

SELECT formatQuerySingleLine('SELECT 1 = `any`((SELECT 1))');
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT 1 = `any`((SELECT 1))'))
     = formatQuerySingleLine('SELECT 1 = `any`((SELECT 1))');

-- Quoting is by exact name, in any case; a name that merely starts like it is left alone.
SELECT formatQuerySingleLine('SELECT 1 = `ANY`((SELECT 1))');
SELECT formatQuerySingleLine('SELECT 1 = anyLast((SELECT 1))');

-- A stored definition carries the same text, so an unquoted name changes what the view means.
SELECT formatQuerySingleLine('CREATE VIEW v AS SELECT 1 = `any`((SELECT 1))');

-- The nulls modifier is parsed after the argument list, so the unquoted text does not parse back at all.
SELECT formatQuerySingleLine('SELECT 1 = `any`((SELECT 1)) RESPECT NULLS');

-- The name is quoted by shape rather than by context, so it is also quoted where the quantifier could not be
-- parsed anyway. Quoting a function name never changes name resolution, so this stays a faithful round-trip.
SELECT formatQuerySingleLine('SELECT any((SELECT 1))');

-- The aggregate keeps its bare name everywhere else: the quantifier needs a single subquery argument, so
-- neither an ordinary call nor a second argument can be read back as one.
SELECT formatQuerySingleLine('SELECT any(number) FROM numbers(3)');
SELECT formatQuerySingleLine('SELECT 1 = any((SELECT 1), 2)');
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT 1 = any((SELECT 1), 2)'))
     = formatQuerySingleLine('SELECT 1 = any((SELECT 1), 2)');

-- `ALL` is the quantifier paired with `ANY`; it is no function name and is quoted unconditionally.
SELECT formatQuerySingleLine('SELECT 1 = `all`((SELECT 1))');

-- `any_value` and `first_value` are registered aliases of `any`, and a stored definition is canonicalised to
-- `any` after the parse-time format check has already run, so the quoting has to hold for a definition whose
-- text never contained the name `any`.
DROP VIEW IF EXISTS v_any_alias;
CREATE VIEW v_any_alias AS SELECT 1 = first_value((SELECT 1));
SELECT position(create_table_query, '= `any`((SELECT 1))') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_any_alias';
DROP VIEW v_any_alias;

-- The formatter itself does not canonicalise, so an alias spelling keeps its own bare name.
SELECT formatQuerySingleLine('SELECT 1 = any_value((SELECT 1))');
