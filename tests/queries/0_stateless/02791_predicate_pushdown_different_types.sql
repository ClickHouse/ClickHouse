# These queries triggered a crash in old ClickHouse versions:

CREATE TEMPORARY TABLE a (key UInt32, ID LowCardinality(String));
CREATE TEMPORARY TABLE b (key UInt32);
SELECT * FROM b JOIN a USING (key) WHERE ID = '1' HAVING ID = '1';

# PS. Predicate pushdown does not work for LowCardinality(String), but it's another problem.
