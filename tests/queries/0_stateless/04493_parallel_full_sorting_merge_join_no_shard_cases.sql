-- The `parallel_full_sorting_merge` join algorithm shards a full sorting merge join by the hash of the
-- join keys. Two cases must NOT be sharded, and must keep returning correct results:
--   1. `ASOF` joins - the trailing key is an inequality key, so hashing by the whole key list would scatter
--      candidate rows with the same equality key but different `ASOF` values into different shards, and the
--      per-shard merge join could miss the closest match.
--   2. Inputs where read-in-order (with `read_in_order_use_virtual_row = 1`) has turned the pre-join sort
--      into a partial `FinishSorting` that emits virtual rows, which the scattered full sort cannot consume.
-- In both cases the optimizer must fall back to a single merge join and still match the `hash` algorithm.

DROP TABLE IF EXISTS pfsmj_asof_left;
DROP TABLE IF EXISTS pfsmj_asof_right;

CREATE TABLE pfsmj_asof_left (key UInt64, t UInt64) ENGINE = MergeTree ORDER BY (key, t);
CREATE TABLE pfsmj_asof_right (key UInt64, t UInt64, v UInt64) ENGINE = MergeTree ORDER BY (key, t);

-- Each right (key, t) pair is unique (number = key * 200 + t), so `ASOF` has no ties and the result is
-- deterministic across algorithms. Insert as several parts to produce multiple streams.
INSERT INTO pfsmj_asof_right SELECT intDiv(number, 200) AS key, number % 200 AS t, number AS v FROM numbers(0, 10000);
INSERT INTO pfsmj_asof_right SELECT intDiv(number, 200) AS key, number % 200 AS t, number AS v FROM numbers(10000, 10000);
-- Left probes many `t` values per key; the closest right row with `r.t <= l.t` must be found.
INSERT INTO pfsmj_asof_left SELECT number % 100 AS key, (number * 7) % 250 AS t FROM numbers(0, 15000);
INSERT INTO pfsmj_asof_left SELECT number % 100 AS key, (number * 7) % 250 AS t FROM numbers(15000, 15000);

-- `max_threads > 1` forces more than one shard so the sharding path is exercised on any runner.
SELECT 'asof_inner',
    (SELECT (sum(r.v), count()) FROM pfsmj_asof_left AS l ASOF JOIN pfsmj_asof_right AS r ON l.key = r.key AND l.t >= r.t SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(r.v), count()) FROM pfsmj_asof_left AS l ASOF JOIN pfsmj_asof_right AS r ON l.key = r.key AND l.t >= r.t SETTINGS join_algorithm = 'hash');

SELECT 'asof_left',
    (SELECT (sum(r.v), count()) FROM pfsmj_asof_left AS l ASOF LEFT JOIN pfsmj_asof_right AS r ON l.key = r.key AND l.t >= r.t SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(r.v), count()) FROM pfsmj_asof_left AS l ASOF LEFT JOIN pfsmj_asof_right AS r ON l.key = r.key AND l.t >= r.t SETTINGS join_algorithm = 'hash');

DROP TABLE pfsmj_asof_left;
DROP TABLE pfsmj_asof_right;

DROP TABLE IF EXISTS pfsmj_rio_left;
DROP TABLE IF EXISTS pfsmj_rio_right;

-- Both sides are ordered by the join key, so read-in-order optimizes the pre-join sort into a partial sort.
CREATE TABLE pfsmj_rio_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pfsmj_rio_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

-- Several parts so read-in-order emits per-part virtual rows.
INSERT INTO pfsmj_rio_left SELECT number, number FROM numbers(0, 30000);
INSERT INTO pfsmj_rio_left SELECT number, number FROM numbers(30000, 30000);
INSERT INTO pfsmj_rio_right SELECT number % 40000, number * 2 FROM numbers(0, 30000);
INSERT INTO pfsmj_rio_right SELECT number % 40000, number * 2 FROM numbers(30000, 30000);

-- With read-in-order and virtual rows enabled, `parallel_full_sorting_merge` must still match `hash`
-- (and must not fail): the optimizer skips scattering when the sort is not a plain full sort.
SELECT 'read_in_order_virtual_row',
    (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_full_sorting_merge', optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, max_threads = 4)
  = (SELECT (sum(l.a + r.b), count()) FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

DROP TABLE pfsmj_rio_left;
DROP TABLE pfsmj_rio_right;
