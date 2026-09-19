SELECT 'basic mode';
SELECT mode(x), toTypeName(mode(x))
FROM VALUES('x UInt8', (1), (2), (2), (3));

SELECT mode(x) IN (1, 2)
FROM VALUES('x UInt8', (1), (1), (2), (2));

SELECT 'nulls and empty input';
SELECT mode(x), toTypeName(mode(x))
FROM VALUES('x Nullable(UInt8)', (NULL), (1), (1), (2));
SELECT mode(x), toTypeName(mode(x))
FROM VALUES('x Nullable(UInt8)', (NULL), (NULL));
SELECT mode(number), toTypeName(mode(number)) FROM numbers(0);
SELECT modeOrNull(number), toTypeName(modeOrNull(number)) FROM numbers(0);

SELECT 'filtered input';
SELECT modeIf(x, keep)
FROM VALUES('x UInt8, keep UInt8', (1, 1), (2, 1), (2, 0), (2, 1), (3, 0));
SELECT modeIf(x, 0) FROM VALUES('x UInt8', (1), (2));

SELECT 'partial states';
SELECT modeMerge(state) IN (1, 2)
FROM
(
    SELECT modeState(x) AS state FROM VALUES('x UInt8', (1), (1))
    UNION ALL
    SELECT modeState(x) AS state FROM VALUES('x UInt8', (2), (2))
);
SELECT modeMerge(state)
FROM
(
    SELECT modeState(x) AS state FROM VALUES('x String', ('a'), ('b'), ('b'))
    UNION ALL
    SELECT modeState(x) AS state FROM VALUES('x String', ('b'), ('c'))
);

SELECT 'distinct combinator';
SELECT modeDistinct(x) IN (1, 2)
FROM VALUES('x UInt8', (1), (1), (2), (2));

SELECT 'generic values';
SELECT mode(x), toTypeName(mode(x))
FROM VALUES('x String', ('a'), ('b'), ('b'), ('c'));
SELECT mode(x), toTypeName(mode(x))
FROM VALUES('x Array(UInt8)', ([1]), ([2]), ([2]), ([3]));
SELECT mode(x), toTypeName(mode(x))
FROM VALUES('x Tuple(UInt8, String)', ((1, 'a')), ((2, 'b')), ((2, 'b')));
SELECT toTypeName(mode(toLowCardinality(toString(number % 2))))
FROM numbers(3);

SELECT 'floating point zero';
SELECT mode(x) = 0
FROM VALUES('x Float32', (0.0), (0.0), (-0.0), (-0.0), (1.0), (1.0), (1.0));
SELECT mode(x) = 0
FROM VALUES('x Float64', (0.0), (0.0), (-0.0), (-0.0), (1.0), (1.0), (1.0));

SELECT 'other numeric types';
SELECT mode(x), toTypeName(mode(x))
FROM VALUES('x Decimal64(2)', (1.25), (2.50), (2.50), (3.75));
SELECT mode(toDate(number % 2)), toTypeName(mode(toDate(number % 2))) FROM numbers(3);
SELECT toTypeName(mode(x)) = 'Date32'
FROM VALUES('x Date32', ('2020-01-01'), ('2020-01-02'), ('2020-01-02'));
SELECT toTypeName(mode(x)) = 'DateTime64(3)'
FROM VALUES('x DateTime64(3)', ('2020-01-01 00:00:00.000'), ('2020-01-01 00:00:01.000'), ('2020-01-01 00:00:01.000'));

SELECT 'persisted states';
DROP TABLE IF EXISTS mode_state_05220;
CREATE TABLE mode_state_05220
(
    k UInt8,
    numeric_state AggregateFunction(mode, UInt64),
    string_state AggregateFunction(mode, String)
)
ENGINE = AggregatingMergeTree
ORDER BY k;
INSERT INTO mode_state_05220
SELECT 1, modeState(x), modeState(toString(x))
FROM VALUES('x UInt64', (1), (1), (2));
INSERT INTO mode_state_05220
SELECT 1, modeState(x), modeState(toString(x))
FROM VALUES('x UInt64', (2), (2), (3));
SELECT k, modeMerge(numeric_state), modeMerge(string_state)
FROM mode_state_05220
GROUP BY k
ORDER BY k;
OPTIMIZE TABLE mode_state_05220 FINAL;
SELECT k, modeMerge(numeric_state), modeMerge(string_state)
FROM mode_state_05220
GROUP BY k
ORDER BY k;
DROP TABLE mode_state_05220;

SELECT 'array combinator';
SELECT modeArray(x)
FROM VALUES('x Array(UInt8)', ([1, 2]), ([2, 3]), ([2, 4]));

SELECT 'window aggregation';
SELECT k, x, mode(x) OVER (PARTITION BY k)
FROM VALUES('k UInt8, x UInt8', (1, 1), (1, 2), (1, 2), (2, 3), (2, 4), (2, 4))
ORDER BY k, x;
