-- The join runtime filter and the shuffle of a distributed join hash the keys extracted by
-- `JoinStepLogical::preCalculateKeys` at their least supertype. A `FixedString` key compared with a `String`
-- key must be normalized there as well, otherwise the filter built on `'a'` rejects the probe row `'a\0\0'`.

SET enable_join_runtime_filters = 1;
SET join_algorithm = 'hash';
-- Keep the sides as written: the join order optimization could hide the failing orientation.
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS fs;
DROP TABLE IF EXISTS s;

CREATE TABLE fs (x FixedString(3), i UInt8) ENGINE = MergeTree ORDER BY i;
INSERT INTO fs VALUES ('a', 1), ('ab', 1), ('a\0b', 1), ('abc', 1), ('', 1);

-- No value of `s` equals the trimmed bytes of a `FixedString` from `fs`: every match differs in trailing zero bytes.
CREATE TABLE s (y String, j UInt8) ENGINE = MergeTree ORDER BY j;
INSERT INTO s VALUES ('a\0\0', 0), ('a\0\0\0\0', 0), ('a\0b\0', 0), ('abcd', 0), ('\0', 0), ('\0\0\0\0', 0), ('ab\0\0', 0), ('abd', 0);

SELECT '-- oracle: operator on the cross product';
SELECT countIf(x = y), countIf(x != y) FROM fs, s;

SELECT '-- inner, both orientations';
SELECT count() FROM fs JOIN s ON fs.x = s.y;
SELECT count() FROM s JOIN fs ON s.y = fs.x;
SELECT count() FROM fs JOIN s ON fs.x = s.y SETTINGS join_algorithm = 'parallel_hash';
SELECT count() FROM s JOIN fs ON s.y = fs.x SETTINGS join_algorithm = 'parallel_hash';

SELECT '-- semi / anti, both orientations';
SELECT count() FROM fs LEFT SEMI JOIN s ON fs.x = s.y;
SELECT count() FROM s LEFT SEMI JOIN fs ON s.y = fs.x;
SELECT count() FROM fs LEFT ANTI JOIN s ON fs.x = s.y;
SELECT count() FROM s LEFT ANTI JOIN fs ON s.y = fs.x;

SELECT '-- multiple keys: LEFT ANTI JOIN builds a single filter on the key tuple';
SELECT count() FROM fs LEFT ANTI JOIN s ON fs.x = s.y AND fs.i = s.j + 1;
SELECT count() FROM s LEFT ANTI JOIN fs ON s.y = fs.x AND s.j + 1 = fs.i;

SELECT '-- Nullable and LowCardinality wrappers';
SELECT count() FROM (SELECT toNullable(y) AS y FROM s) AS sn JOIN fs ON sn.y = fs.x;
SELECT count() FROM (SELECT toLowCardinality(y) AS y FROM s) AS sl JOIN fs ON sl.y = fs.x;
SELECT count() FROM s JOIN (SELECT toNullable(x) AS x FROM fs) AS fsn ON s.y = fsn.x;
SELECT count() FROM s JOIN (SELECT toLowCardinality(x) AS x FROM fs) AS fsl ON s.y = fsl.x;

SELECT '-- the runtime filter is built and applied on the normalized keys';
SELECT replaceRegexpOne(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' from \\w+\\.\\w+', '')
FROM (EXPLAIN actions = 1 SELECT count() FROM s JOIN fs ON s.y = fs.x)
WHERE explain LIKE '%Build runtime join filter on%' OR explain LIKE '%Filter column: RF%';

DROP TABLE fs;
DROP TABLE s;
