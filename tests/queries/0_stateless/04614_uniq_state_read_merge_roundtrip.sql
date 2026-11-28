DROP TABLE IF EXISTS t_uniq_states;
CREATE TABLE t_uniq_states (k UInt64, s AggregateFunction(uniq, UInt64)) ENGINE = MergeTree ORDER BY k;

-- numbers(258) covers state sizes up to 257, crossing the 256-element chunk boundary of UniquesHashSet::read.
INSERT INTO t_uniq_states SELECT number, arrayReduce('uniqState', arrayMap(x -> toUInt64(x), range(number))) FROM numbers(258);

SELECT sum(k = finalizeAggregation(s)) FROM t_uniq_states;
SELECT uniqMerge(s) FROM t_uniq_states;

-- Merges where the destination saturates skip_degree.
SELECT uniqMerge(s) FROM (SELECT uniqState(number) AS s FROM numbers(1000000) GROUP BY number % 16) SETTINGS max_threads = 1;
SELECT uniqMerge(s) FROM (SELECT uniqState(number) AS s FROM numbers(1000000) GROUP BY number >= 999000) SETTINGS max_threads = 1;

DROP TABLE t_uniq_states;
