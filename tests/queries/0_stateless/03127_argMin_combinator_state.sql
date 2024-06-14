SELECT toTypeName(sumArgMinState(number, number)) FROM numbers(1);
SELECT sumArgMinState(number, number) AS a FROM numbers(3) FORMAT Null;

DROP TABLE IF EXISTS argmax_comb;
CREATE TABLE argmax_comb(
        id UInt64,
        state AggregateFunction(avgArgMax, Float64, UInt64)
    )
    ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO argmax_comb
    SELECT
        CAST(number % 10, 'UInt64') AS id,
        avgArgMaxState(CAST(number, 'Float64'), id)
    FROM numbers(100)
    GROUP BY id;
SELECT avgArgMaxMerge(state) FROM argmax_comb;
SELECT
    id,
    avgArgMaxMerge(state)
FROM argmax_comb
GROUP BY id
ORDER BY id ASC;