SET enable_analyzer=1;

CREATE TABLE with_default (a Int64 DEFAULT 1) Engine=Memory();
-- This case is failing with old analyzer, because "_subquery" name prefix has a special treatment.
CREATE TABLE with_clashing_name (_subquery_test Int64 DEFAULT 7) Engine=Memory();
CREATE TABLE with_dependent_defaults (a Int64 DEFAULT 7, b Int64 DEFAULT a + 7, c Int64 DEFAULT a * b) Engine=Memory();
CREATE TABLE with_regular_column (a Int64, b Int64 DEFAULT 7, c Int64 DEFAULT a * b) Engine=Memory();

SET insert_null_as_default=1;

INSERT INTO with_default SELECT NULL FROM system.one;
INSERT INTO with_clashing_name SELECT NULL FROM system.one;
INSERT INTO with_dependent_defaults SELECT NULL, NULL, NULL FROM system.one;
INSERT INTO with_regular_column SELECT 2, NULL, NULL FROM system.one;

-- { echoOn }
SELECT * FROM with_default ORDER BY ALL;
SELECT * FROM with_clashing_name ORDER BY ALL;
SELECT * FROM with_dependent_defaults ORDER BY ALL;
SELECT * FROM with_regular_column ORDER BY ALL;
