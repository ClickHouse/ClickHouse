SELECT count() FROM numbers(10000) AS l
INNER JOIN (SELECT number FROM numbers(10000)) AS r
ON l.number = r.number AND l.number > 0 AND r.number > 0
SETTINGS enable_join_runtime_filters = 0;

SELECT 'dedup hits',
       countIf(explain LIKE '%FUNCTION greater(number%-> greater(__table2.number, 0_UInt8)%')
FROM (
    EXPLAIN actions = 1
    SELECT number FROM numbers(10000) AS l
    INNER JOIN (SELECT number FROM numbers(10000)) AS r
    ON l.number = r.number AND l.number > 0 AND r.number > 0
    SETTINGS enable_join_runtime_filters = 0
);

SELECT count() FROM (SELECT 1 AS x WHERE x = 1 AND 1 = x);

SELECT count() FROM numbers(100) WHERE number > 0 AND number > 0;

SELECT count() FROM numbers(100) WHERE rand() % 256 < 256 AND rand() % 256 < 256;

-- same predicate appears with the inner alias and the outer alias
-- ALIAS look-through during keying collapses them despite different alias chains
DROP TABLE IF EXISTS u_4263;
CREATE TABLE u_4263 (uid Int16) ENGINE = MergeTree ORDER BY uid;
INSERT INTO u_4263 VALUES (10), (20);

SELECT count() FROM (SELECT * FROM u_4263 WHERE uid = 10) WHERE uid = 10;

SELECT 'one equals + one const',
       countIf(explain LIKE '%FUNCTION equals(uid%'),
       countIf(explain LIKE '%COLUMN Const(UInt8) -> 10_UInt8%')
FROM (
    EXPLAIN actions = 1
    SELECT * FROM (SELECT * FROM u_4263 WHERE uid = 10) WHERE uid = 10
    SETTINGS optimize_move_to_prewhere = 0
);

DROP TABLE u_4263;
