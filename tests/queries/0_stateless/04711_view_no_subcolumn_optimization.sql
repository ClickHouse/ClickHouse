DROP TABLE IF EXISTS vstr;
DROP TABLE IF EXISTS vw;
DROP TABLE IF EXISTS tgt;
DROP TABLE IF EXISTS okv;

-- Every row pins `optimize_functions_to_subcolumns` explicitly: the CI runner randomizes it, and a
-- run that injects 0 would silently disarm the rows that must observe the optimization firing.

-- `FunctionToSubcolumnsPass` exists only in the analyzer, so every assertion below needs it
-- enabled. The `old analyzer` CI job turns it off in the server profile.
SET enable_analyzer = 1;

-- The plan text below is grepped, so the rendering has to be pinned: `pretty` (the default)
-- renders a different shape from `legacy`.
SET explain_query_plan_default = 'legacy';

-- A view whose declared column type differs from what its inner query produces.
CREATE TABLE vstr (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO vstr VALUES ('[1,2,3,4,5,6]');
CREATE VIEW vw (arr Array(UInt8)) AS SELECT arr FROM vstr;

-- The optimizer must not rewrite `length(arr)` into a read of `arr.size0`: the name is
-- forwarded into the inner query, where `arr` is a `String` and has no `.size0` subcolumn.
SELECT sum(length(arr)) FROM vw SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(length(arr)) FROM vw SETTINGS optimize_functions_to_subcolumns = 0;

-- A subcolumn the inner query genuinely lacks must still be rejected.
SELECT sum(arr.size0) FROM vw SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError UNKNOWN_IDENTIFIER }

-- A type-consistent view keeps working.
CREATE TABLE tgt (arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tgt VALUES ([1,2]), ([3]);
CREATE VIEW okv AS SELECT arr FROM tgt;

SELECT sum(length(arr)) FROM okv SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(arr.size0) FROM okv SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(length(arr)) FROM okv SETTINGS optimize_functions_to_subcolumns = 0;

-- The optimization still fires when reading the table directly.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT sum(length(arr)) FROM tgt SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%arr.size0%';

-- Reading through a view does not request the subcolumn from the inner query.
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT sum(length(arr)) FROM okv SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%arr.size0%';
-- Positive control for the row above: the un-rewritten `length` call IS in that same plan, so the
-- absent-substring assertion cannot pass merely because nothing rendered.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT sum(length(arr)) FROM okv SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%length%';

DROP TABLE okv;
DROP TABLE tgt;
DROP TABLE vw;
DROP TABLE vstr;
