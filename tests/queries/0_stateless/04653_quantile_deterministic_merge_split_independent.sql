-- `quantileDeterministic` must return the same value for the same data no matter how that data was
-- split between the partial aggregate states that are merged to produce it. Storing the states in a
-- table is what makes them go through serialization, which used to drop the reservoir's skip degree:
-- a state thinned out to one sample per 2^7 rows was then indistinguishable from a complete one, so a
-- merge under-weighted the state that had seen the most rows. Under parallel replicas or a distributed
-- query the split varies between runs, which made the function non-deterministic in practice.

DROP TABLE IF EXISTS quantile_deterministic_states;

CREATE TABLE quantile_deterministic_states
(
    split UInt8,
    part UInt8,
    state AggregateFunction(medianDeterministic, UInt64, UInt64)
)
ENGINE = MergeTree ORDER BY (split, part);

-- Two equal halves.
INSERT INTO quantile_deterministic_states SELECT 1, 0, medianDeterministicState(number, number) FROM numbers(500000);
INSERT INTO quantile_deterministic_states SELECT 1, 1, medianDeterministicState(number, number) FROM numbers(500000, 500000);

-- A very lopsided split: this is the one that used to give a different answer.
INSERT INTO quantile_deterministic_states SELECT 2, 0, medianDeterministicState(number, number) FROM numbers(990000);
INSERT INTO quantile_deterministic_states SELECT 2, 1, medianDeterministicState(number, number) FROM numbers(990000, 10000);

-- Ten equal pieces.
INSERT INTO quantile_deterministic_states
    SELECT 3, toUInt8(intDiv(number, 100000)) AS part, medianDeterministicState(number, number)
    FROM numbers(1000000) GROUP BY part;

-- One piece of every size at once.
INSERT INTO quantile_deterministic_states SELECT 4, 0, medianDeterministicState(number, number) FROM numbers(1);
INSERT INTO quantile_deterministic_states SELECT 4, 1, medianDeterministicState(number, number) FROM numbers(1, 999);
INSERT INTO quantile_deterministic_states SELECT 4, 2, medianDeterministicState(number, number) FROM numbers(1000, 9000);
INSERT INTO quantile_deterministic_states SELECT 4, 3, medianDeterministicState(number, number) FROM numbers(10000, 990000);

-- Split 0 is the value a single state over all the rows gives; every other split must match it.
SELECT split, median
FROM
(
    SELECT 0 AS split, medianDeterministic(number, number) AS median FROM numbers(1000000)
    UNION ALL
    SELECT split, medianDeterministicMerge(state) AS median FROM quantile_deterministic_states GROUP BY split
)
ORDER BY split;

DROP TABLE quantile_deterministic_states;
