-- `has(A, e)` asks whether `e` is an element of `A`, so a constant FIRST argument is a set of
-- candidate values for the whole column, not one element of it. A `bloom_filter` over an array
-- column and a text bloom filter over `mapKeys` hold one hash per element or per key term, so they
-- cannot answer that shape and must not be applied to it.
-- Every query below runs with `use_skip_indexes = 1` and then `0`; the two must agree.

DROP TABLE IF EXISTS t_bf_has_string;

CREATE TABLE t_bf_has_string (x Array(String), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_has_string VALUES ([]), (['a']), (['b']), (['c']);

SELECT count() FROM t_bf_has_string WHERE has([['b']], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE has([['b']], x) SETTINGS use_skip_indexes = 0;

-- An empty array among the candidates, and the empty array as the only candidate.
SELECT count() FROM t_bf_has_string WHERE has([[], ['b']], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE has([[], ['b']], x) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_string WHERE has([[]], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE has([[]], x) SETTINGS use_skip_indexes = 0;

-- The forward shape, `indexOf`, `hasAny` in both argument orders, `hasAll` and `IN` share this
-- index condition and keep using the index.
SELECT count() FROM t_bf_has_string WHERE has(x, 'b') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE has(x, 'b') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_string WHERE indexOf(x, 'b') != 0 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE indexOf(x, 'b') != 0 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_string WHERE hasAny(['b'], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE hasAny(['b'], x) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_string WHERE hasAny(x, ['b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE hasAny(x, ['b']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_string WHERE hasAll(x, ['b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE hasAll(x, ['b']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_string WHERE x IN (['b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE x IN (['b']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_string WHERE x IN ([], ['b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_string WHERE x IN ([], ['b']) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_has_string;

-- `Array(LowCardinality(String))`: the column type the AST fuzzer used.
DROP TABLE IF EXISTS t_bf_has_low_cardinality;

CREATE TABLE t_bf_has_low_cardinality (x Array(LowCardinality(String)), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_has_low_cardinality VALUES ([]), (['a']), (['b']), (['c']);

SELECT count() FROM t_bf_has_low_cardinality WHERE has([['b']], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_low_cardinality WHERE has([['b']], x) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_has_low_cardinality;

-- A nullable element type. `Array(Nullable(String))` cannot carry this index at all
-- ("Unexpected type ... of bloom filter index"), so the wrapper is tested through `LowCardinality`.
DROP TABLE IF EXISTS t_bf_has_nullable;

CREATE TABLE t_bf_has_nullable (x Array(LowCardinality(Nullable(String))), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_has_nullable VALUES ([]), (['a']), (['b']), (['c']);

SELECT count() FROM t_bf_has_nullable WHERE has([['b']], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_nullable WHERE has([['b']], x) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_has_nullable;

-- A numeric element type: converting the whole constant array to `UInt32` raised
-- `TYPE_MISMATCH` instead of returning wrong results.
DROP TABLE IF EXISTS t_bf_has_uint;

CREATE TABLE t_bf_has_uint (x Array(UInt32), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_has_uint VALUES ([]), ([1]), ([2]), ([3]);

SELECT count() FROM t_bf_has_uint WHERE has([[2]], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_uint WHERE has([[2]], x) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_has_uint;

-- A `FixedString` element type, where the constant is compared in its padded form.
DROP TABLE IF EXISTS t_bf_has_fixed_string;

CREATE TABLE t_bf_has_fixed_string (x Array(FixedString(2)), INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_has_fixed_string VALUES ([]), (['aa']), (['bb']), (['cc']);

SELECT count() FROM t_bf_has_fixed_string WHERE has([['bb']], x) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_fixed_string WHERE has([['bb']], x) SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_has_fixed_string;

-- A `mapKeys` index, where the constant is a set of candidate maps. `bloom_filter` returned wrong
-- results and the text bloom filters raised `BAD_GET`; `tokenbf_v1`, `ngrambf_v1` and
-- `sparse_grams` share one index condition, so one of them covers all three.
DROP TABLE IF EXISTS t_bf_has_map_keys;

CREATE TABLE t_bf_has_map_keys (m Map(String, UInt32), INDEX idx_m mapKeys(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_has_map_keys VALUES (map('a', 1)), (map('b', 2)), (map('c', 3)), (map('d', 4));

SELECT count() FROM t_bf_has_map_keys WHERE has([map('b', 2)], m) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_map_keys WHERE has([map('b', 2)], m) SETTINGS use_skip_indexes = 0;

-- The map adapters and the forward `has` over a `Map` keep using the index.
SELECT count() FROM t_bf_has_map_keys WHERE mapContainsKey(m, 'b') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_map_keys WHERE mapContainsKey(m, 'b') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_has_map_keys WHERE has(m, 'b') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf_has_map_keys WHERE has(m, 'b') SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_has_map_keys;

DROP TABLE IF EXISTS t_tokenbf_has_map_keys;

CREATE TABLE t_tokenbf_has_map_keys (m Map(String, String), INDEX idx_m mapKeys(m) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_tokenbf_has_map_keys VALUES (map('k', 'a')), (map('k', 'b')), (map('k', 'c')), (map('k', 'd'));

SELECT count() FROM t_tokenbf_has_map_keys WHERE has([map('k', 'b')], m) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_tokenbf_has_map_keys WHERE has([map('k', 'b')], m) SETTINGS use_skip_indexes = 0;

-- A constant compared with `equals` carries no role, in either argument order, and still prunes.
SELECT count() FROM t_tokenbf_has_map_keys WHERE 'b' = m['k'] SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_tokenbf_has_map_keys WHERE 'b' = m['k'] SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_tokenbf_has_map_keys WHERE mapContainsKey(m, 'k') SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_tokenbf_has_map_keys WHERE mapContainsKey(m, 'k') SETTINGS use_skip_indexes = 0;

DROP TABLE t_tokenbf_has_map_keys;

-- `has(<constant array>, <indexed scalar>)` is the one shape a constant first argument is usable
-- for: the index holds a hash of each whole value, so the candidates can be probed directly.
DROP TABLE IF EXISTS t_bf_has_scalar;

CREATE TABLE t_bf_has_scalar (s String, INDEX idx_s s TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_bf_has_scalar VALUES (''), ('a'), ('b'), ('c');

SELECT count() FROM t_bf_has_scalar WHERE has(['b'], s) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT count() FROM t_bf_has_scalar WHERE has(['b'], s) SETTINGS use_skip_indexes = 0, optimize_rewrite_has_to_in = 0;

DROP TABLE t_bf_has_scalar;

DROP TABLE IF EXISTS t_tokenbf_has_scalar;

CREATE TABLE t_tokenbf_has_scalar (s String, INDEX idx_s s TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_tokenbf_has_scalar VALUES (''), ('a'), ('b'), ('c');

SELECT count() FROM t_tokenbf_has_scalar WHERE has(['b'], s) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT count() FROM t_tokenbf_has_scalar WHERE has(['b'], s) SETTINGS use_skip_indexes = 0, optimize_rewrite_has_to_in = 0;

DROP TABLE t_tokenbf_has_scalar;

-- The shapes that keep the index must still reach it and still prune granules. How many survive is
-- not asserted: `index_granularity_bytes` is randomized and a bloom filter's false positives are a
-- draw, so only "fewer than all" is stable.
DROP TABLE IF EXISTS t_bf_has_pruning;

CREATE TABLE t_bf_has_pruning (s String, x Array(String), y UInt32, INDEX idx_s s TYPE bloom_filter GRANULARITY 1,
  INDEX idx_x x TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY y SETTINGS index_granularity = 8192;
INSERT INTO t_bf_has_pruning SELECT toString(number), [toString(number)], number FROM numbers(200000);

SELECT countIf(explain LIKE '%Name: idx_s%') > 0 AND countIf(toUInt64OrZero(g[1]) < toUInt64OrZero(g[2])) > 0
FROM (
    SELECT explain, splitByChar('/', extract(explain, 'Granules: ([0-9]+/[0-9]+)')) AS g
    FROM (EXPLAIN indexes = 1 SELECT sum(y) FROM t_bf_has_pruning WHERE has(['nosuchvalue'], s)
          SETTINGS optimize_rewrite_has_to_in = 0)
);

SELECT countIf(explain LIKE '%Name: idx_x%') > 0 AND countIf(toUInt64OrZero(g[1]) < toUInt64OrZero(g[2])) > 0
FROM (
    SELECT explain, splitByChar('/', extract(explain, 'Granules: ([0-9]+/[0-9]+)')) AS g
    FROM (EXPLAIN indexes = 1 SELECT sum(y) FROM t_bf_has_pruning WHERE has(x, 'nosuchvalue'))
);

DROP TABLE t_bf_has_pruning;
