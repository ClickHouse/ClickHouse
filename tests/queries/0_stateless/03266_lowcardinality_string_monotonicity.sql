DROP TABLE IF EXISTS test_lc_pk;
CREATE TABLE test_lc_pk (s String) engine = MergeTree ORDER BY s;

INSERT INTO test_lc_pk SELECT toString(number) FROM numbers(1e6);

WITH explain_table AS
    (
        SELECT
            explain,
            rowNumberInAllBlocks() AS rn
        FROM viewExplain('EXPLAIN', 'indexes = 1', (
            SELECT count()
            FROM test_lc_pk
            WHERE CAST(s, 'LowCardinality(String)') = '42'
        ))
    )
SELECT trimLeft(e2.explain) AS keys_value
FROM explain_table AS e1
INNER JOIN explain_table AS e2 ON e2.rn = (e1.rn + 1)
WHERE e1.explain ILIKE '%Keys%'; -- We basically try to verify that we have our column as the key in explain indexes (we don't read all data)

WITH explain_table AS
    (
        SELECT
            explain,
            rowNumberInAllBlocks() AS rn
        FROM viewExplain('EXPLAIN', 'indexes = 1', (
            SELECT count()
            FROM test_lc_pk
            WHERE CAST(s, 'String') = '42'
        ))
    )
SELECT trimLeft(e2.explain) AS keys_value
FROM explain_table AS e1
INNER JOIN explain_table AS e2 ON e2.rn = (e1.rn + 1)
WHERE e1.explain ILIKE '%Keys%';

DROP TABLE test_lc_pk;
