-- https://github.com/ClickHouse/ClickHouse/issues/117206
-- `caseWithExpression` is lowered to `transform`, whose mapping table dropped every `Enum` member
-- whose numeric value does not parse out of its name when the expression is `Nullable(Enum)`.

DROP TABLE IF EXISTS t_case_nullable_enum;
CREATE TABLE t_case_nullable_enum (a Nullable(Enum8('A' = 0, 'B' = 1))) ENGINE = Memory;
INSERT INTO t_case_nullable_enum VALUES (NULL), ('A'), ('B');

SELECT a, caseWithExpression(a, 'A', 'a', 'B', 'b', NULL) FROM t_case_nullable_enum ORDER BY a NULLS FIRST;
SELECT a, transform(a, ['A', 'B'], ['a', 'b'], NULL) FROM t_case_nullable_enum ORDER BY a NULLS FIRST;

-- A `NULL` mapping entry still matches a `NULL` input row.
SELECT a, transform(a, [NULL, 'B'], ['n', 'b'], 'z') FROM t_case_nullable_enum ORDER BY a NULLS FIRST;

-- A numeric mapping value is cast to the `Enum` without a membership check, so a value that is not
-- representable as a member must not become one.
SELECT transform(CAST(materialize('B') AS Enum8('A' = 0, 'B' = 1)), [1.5], ['x'], 'z');

DROP TABLE t_case_nullable_enum;
