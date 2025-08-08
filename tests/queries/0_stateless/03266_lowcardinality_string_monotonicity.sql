DROP TABLE IF EXISTS test_lc_pk;
CREATE TABLE test_lc_pk (s String) engine = MergeTree ORDER BY s;

INSERT INTO test_lc_pk SELECT toString(number) FROM numbers(1e6);

SELECT trimLeft(explain)
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', 'indexes = 1', (
        SELECT count()
        FROM test_lc_pk
        WHERE CAST(s, 'LowCardinality(String)') = '42'
    ))
)
WHERE explain LIKE '%Condition%'; -- We basically try to verify that we have our column as the key in explain indexes (we don't read all data)

SELECT trimLeft(explain)
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', 'indexes = 1', (
        SELECT count()
        FROM test_lc_pk
        WHERE CAST(s, 'String') = '42'
    ))
)
WHERE explain LIKE '%Condition%';

DROP TABLE test_lc_pk;
