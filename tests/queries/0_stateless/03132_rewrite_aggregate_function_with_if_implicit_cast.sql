SET enable_analyzer = 1;
-- { echoOn }
SELECT concat(1, sum(if(0, toUInt128(concat('%', toLowCardinality(toNullable(1)), toUInt256(1))), materialize(0))));
SELECT any(if((number % 10) = 5, number, CAST(NULL, 'Nullable(Int128)'))) AS a, toTypeName(a) FROM numbers(100) AS a;
EXPLAIN QUERY TREE SELECT any(if((number % 10) = 5, number, CAST(NULL, 'Nullable(Int128)'))) AS a, toTypeName(a) FROM numbers(100);

SELECT any(if((number % 10) = 5, CAST(NULL, 'Nullable(Int128)'), number)) AS a, toTypeName(a) FROM numbers(100) AS a;
EXPLAIN QUERY TREE SELECT any(if((number % 10) = 5, CAST(NULL, 'Nullable(Int128)'), number)) AS a, toTypeName(a) FROM numbers(100);
