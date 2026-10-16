-- A tuple LHS with a bare string column RHS is a one-element set whose value is parsed
-- into the tuple type, like the set built from a column: a non-parseable value raises a
-- parsing error (test 04005_merge_table_virtual_column_filter_bad_cast pins that case
-- over a merge table), not NO_COMMON_TYPE, while a parseable value is compared.

SET enable_analyzer = 1;

SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize('(''a'',''b'')') AS rhs);
SELECT ('a', 'b') NOT IN (rhs) FROM (SELECT materialize('(''x'',''y'')') AS rhs);
SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize('(''x'',''y'')') AS rhs);
SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize(toLowCardinality('(''a'',''b'')')) AS rhs);
SELECT ('a', 'b') IN (rhs) FROM (SELECT materialize('not a tuple') AS rhs); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
