SET enable_analyzer=1;

CREATE TABLE default (a Int64 DEFAULT 1) Engine=Memory();
-- This case is failing with old analyzer, because "_subquery" name prefix has a special treatment.
CREATE TABLE clashing_name (_subquery_test Int64 DEFAULT 7) Engine=Memory();
CREATE TABLE dependent_defaults (a Int64 DEFAULT 7, b Int64 DEFAULT a + 7, c Int64 DEFAULT a * b) Engine=Memory();
CREATE TABLE regular_column (a Int64, b Int64 DEFAULT 7, c Int64 DEFAULT a * b) Engine=Memory();
CREATE TABLE complex_name_backward_compat (a Int64, `b.b` Int64 DEFAULT 7, c Int64 DEFAULT a * `b.b`) Engine=Memory();
CREATE TABLE compound_id (a Tuple(x Int64, y Int64), b Int64 DEFAULT a.x + 7) Engine=Memory();

SET insert_null_as_default=1;

INSERT INTO default SELECT NULL FROM system.one;
INSERT INTO clashing_name SELECT NULL FROM system.one;
INSERT INTO dependent_defaults SELECT NULL, NULL, NULL FROM system.one;
INSERT INTO regular_column SELECT 2, NULL, NULL FROM system.one;
INSERT INTO complex_name_backward_compat SELECT 3, NULL, NULL FROM system.one;
INSERT INTO compound_id (a) VALUES ((1, 2));

-- { echoOn }
SELECT * FROM default ORDER BY ALL;
SELECT * FROM clashing_name ORDER BY ALL;
SELECT * FROM dependent_defaults ORDER BY ALL;
SELECT * FROM regular_column ORDER BY ALL;
SELECT * FROM complex_name_backward_compat ORDER BY ALL;
SELECT * FROM compound_id ORDER BY ALL;
