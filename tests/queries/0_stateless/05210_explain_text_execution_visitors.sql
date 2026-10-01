SET enable_global_with_statement = 1;
SET union_default_mode = '';
SET intersect_default_mode = '';
SET except_default_mode = '';

-- Formatting preserves unspecified set-operation modes.
EXPLAIN TEXT SELECT 1 UNION SELECT 2 ONELINE;
EXPLAIN TEXT SELECT 1 INTERSECT SELECT 2 ONELINE;
EXPLAIN TEXT SELECT 1 EXCEPT SELECT 2 ONELINE;

-- Formatting does not propagate `WITH` into another union arm.
EXPLAIN TEXT WITH 1 AS x SELECT x UNION ALL SELECT 2 ONELINE;

-- Explicit actions preserve unspecified set-operation modes.
EXPLAIN TEXT (SELECT 1 UNION SELECT 2) MODIFY FORMAT CSV, ONELINE;
