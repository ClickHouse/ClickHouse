-- Tags: shard

-- Tests the packed per-bucket sample storage of the `timeSeries*ToGrid` aggregate functions: a bucket's raw sample vector is traded for a compact delta-encoded blob when the in-order write cursor leaves the bucket (or when the bucket outgrows the self-pack threshold), and the packed form must behave exactly like the raw one on every path: late O(1) appends, out-of-order and duplicate repairs (unpack, replay, repack), in-memory merges of partial states with interleaved and fully overlapping ranges, serialized-state merges, states mixing raw and packed buckets, and the canonical (total-order, arrival-order-independent) collapse of duplicated timestamps carrying NaN or signed zeros.

SET allow_experimental_time_series_aggregate_functions = 1;
SET optimize_aggregation_in_order = 0;

-- Base series: 136 samples 5s apart over (0, 700] with buckets of 20 samples on the grid used below, so every bucket crosses the pack threshold and is packed when the in-order cursor leaves it. The value regimes cover every packed value token: small integer deltas (7), bit-identical repeats, medium integer deltas (500), huge integer deltas (1e7), non-integer deltas (-0.5), specials (NaN, +-Inf, -0.0, a denormal, 1e300), and a 3/4 flip-flop for changes/resets. Indices 76..79 (timestamps 385..400) are left out so the bucket (300, 400] ends early and late samples can exercise the O(1) append to a packed bucket.
DROP TABLE IF EXISTS ts_pack_gen;
CREATE VIEW ts_pack_gen AS
SELECT
    arrayMap(i -> toInt64(5 + 5 * i), idx) AS ts_sec,
    arrayMap(i -> multiIf(
        i < 20, 100. + 7 * i,
        i < 40, 240.,
        i < 60, 240. + 500 * (i - 39),
        i < 80, 1e7 * (i - 59),
        i < 100, 17.25 - 0.5 * (i - 80),
        i = 105, nan,
        i = 107, inf,
        i = 109, -inf,
        i = 111, -0.,
        i = 113, 5e-324,
        i = 115, 1e300,
        i < 120, 42.5,
        3. + (i % 2)), idx) AS vals
FROM (SELECT arrayFilter(i -> i < 76 OR i > 79, range(140)) AS idx);

-- Late samples aimed at the bucket (200, 300], which is packed by the time they arrive: (300, 9999) loses the dedup against the packed last timestamp (the O(1) drop), (250, 6000) is a winning interior duplicate that unpack-repairs the bucket, and the next four - a winning duplicate of the first timestamp, a losing interior duplicate, a brand-new interior timestamp, a timestamp preceding the bucket's samples - replay the raw out-of-order logic on the repaired (now raw) bucket; (385..400) append in O(1) to the packed gap bucket (300, 400] and move the cursor so (200, 300] re-packs, letting (300, 20000) unpack-repair it again as a winning duplicate of its last timestamp.
DROP TABLE IF EXISTS ts_pack_late;
CREATE VIEW ts_pack_late AS
SELECT CAST([(300, 9999.), (250, 6000.), (205, 800.), (260, 1.), (257, 12345.), (202, 5.), (385, 55555.), (390, 55556.), (400, 55557.), (300, 20000.)], 'Array(Tuple(Int64, Float64))') AS late;

-- The expected multiset: the base and late samples unioned, sorted, and deduplicated keeping the larger value per timestamp (every duplicated value here is an ordinary number, so `arrayMax` matches the aggregate's total-order dedup; NaN and signed-zero duplicates get a dedicated scenario below).
DROP TABLE IF EXISTS ts_pack_expected;
CREATE VIEW ts_pack_expected AS
SELECT
    arrayMap(p -> p.1, dedup) AS ts_sec,
    arrayMap(p -> p.2, dedup) AS vals
FROM
(
    SELECT arraySort(p -> p.1, arrayMap(t -> (t, arrayMax(arrayMap(q -> q.2, arrayFilter(q -> q.1 = t, all_pairs)))), arrayDistinct(arrayMap(p -> p.1, all_pairs)))) AS dedup
    FROM (SELECT arrayConcat(arrayZip(ts_sec, vals), (SELECT late FROM ts_pack_late)) AS all_pairs FROM ts_pack_gen)
);

SELECT 'expected results on the sorted unique multiset (rate, increase, delta, changes, resets):';
SELECT
    timeSeriesRateToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS rate,
    timeSeriesIncreaseToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS increase,
    timeSeriesDeltaToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS delta,
    timeSeriesChangesToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS changes,
    timeSeriesResetsToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS resets
FROM ts_pack_expected
FORMAT Vertical;

-- The in-order base plus the late batch: the array form feeds samples in exact array order on a single state, so the base packs its buckets deterministically and each late sample deterministically hits a packed bucket on its intended path.
SELECT 'in-order build with late samples into packed buckets equals the expected multiset (all 1):';
SELECT
    timeSeriesRateToGrid(0, 1000, 100, 200)(l_ts, l_vals) = (SELECT timeSeriesRateToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesIncreaseToGrid(0, 1000, 100, 200)(l_ts, l_vals) = (SELECT timeSeriesIncreaseToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesDeltaToGrid(0, 1000, 100, 200)(l_ts, l_vals) = (SELECT timeSeriesDeltaToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesChangesToGrid(0, 1000, 100, 200)(l_ts, l_vals) = (SELECT timeSeriesChangesToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesResetsToGrid(0, 1000, 100, 200)(l_ts, l_vals) = (SELECT timeSeriesResetsToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesDerivToGrid(0, 1000, 100, 200)(l_ts, l_vals) = (SELECT timeSeriesDerivToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesPredictLinearToGrid(0, 1000, 100, 200, 60)(l_ts, l_vals) = (SELECT timeSeriesPredictLinearToGrid(0, 1000, 100, 200, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected)
FROM
(
    SELECT
        arrayMap(x -> toDateTime64(x, 3, 'UTC'), arrayConcat(ts_sec, arrayMap(p -> p.1, (SELECT late FROM ts_pack_late)))) AS l_ts,
        arrayConcat(vals, arrayMap(p -> p.2, (SELECT late FROM ts_pack_late))) AS l_vals
    FROM ts_pack_gen
);

-- A deterministic shuffle of the whole multiset: constant disorder keeps buckets raw or repairs them right after sealing, covering the normalize-then-pack and the repair-budget paths.
SELECT 'shuffled build equals the expected multiset (all 1):';
SELECT
    timeSeriesRateToGrid(0, 1000, 100, 200)(s_ts, s_vals) = (SELECT timeSeriesRateToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesIncreaseToGrid(0, 1000, 100, 200)(s_ts, s_vals) = (SELECT timeSeriesIncreaseToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesChangesToGrid(0, 1000, 100, 200)(s_ts, s_vals) = (SELECT timeSeriesChangesToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected),
    timeSeriesDerivToGrid(0, 1000, 100, 200)(s_ts, s_vals) = (SELECT timeSeriesDerivToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_expected)
FROM
(
    SELECT
        arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), shuffled) AS s_ts,
        arrayMap(p -> p.2, shuffled) AS s_vals
    FROM
    (
        SELECT arraySort(p -> cityHash64(p.1, p.2), arrayConcat(arrayZip(ts_sec, vals), (SELECT late FROM ts_pack_late))) AS shuffled
        FROM ts_pack_gen
    )
);

-- Duplicated timestamps whose values a plain `max` cannot order - NaN mixed with numbers, -0.0 with +0.0 - resolve to the larger value under the IEEE-754 total order: NaN (sign bit clear) beats numbers and +0.0 beats -0.0. The rule is commutative and associative, so the survivor is the same whether a run collapses on the in-order add, in one sort-and-dedup pass over a shuffle, in staged packed-bucket repairs, or in a state merge; this scenario runs all four and pins the canonical result (the old implementation's survivor depended on arrival order here). The base stream (5s spacing, 20 samples per 100s bucket, so sealed buckets pack) carries NaN duplicates in both arrival orders (150: NaN then number; 250: number then NaN), a NaN at the bucket end 300, and -0.0 at 50; the sample at 400 seals (200, 300], then the late batch throws a number at a packed NaN (unpack-repair at 150, the O(1) losing dup at 300), NaNs at packed numbers (295 interior, 10 after the bucket turned raw), and +0.0 at -0.0 (50, a winner visible only in the state bytes since -0.0 = 0.0 numerically).
DROP TABLE IF EXISTS ts_pack_nan;
CREATE VIEW ts_pack_nan AS
SELECT inorder, late, arrayConcat(inorder, late) AS all_pairs
FROM
(
    SELECT
        arrayConcat(arrayFlatten(arrayMap(i -> multiIf(
            i = 9, [(toInt64(50), -0.)],
            i = 29, [(toInt64(150), nan), (toInt64(150), 222.)],
            i = 49, [(toInt64(250), 333.), (toInt64(250), nan)],
            i = 59, [(toInt64(300), nan)],
            [(toInt64(5 + 5 * i), toFloat64(i))]), range(60))), CAST([(400, 77.)], 'Array(Tuple(Int64, Float64))')) AS inorder,
        CAST([(150, 555.), (300, 123.), (295, nan), (50, 0.), (10, nan)], 'Array(Tuple(Int64, Float64))') AS late
);

SELECT 'canonical dedup of NaN and signed-zero duplicates, in-order build with late samples into packed buckets:';
SELECT
    timeSeriesRateToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), all_pairs), arrayMap(p -> p.2, all_pairs)) AS rate,
    timeSeriesChangesToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), all_pairs), arrayMap(p -> p.2, all_pairs)) AS changes,
    timeSeriesResetsToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), all_pairs), arrayMap(p -> p.2, all_pairs)) AS resets
FROM ts_pack_nan
FORMAT Vertical;

SELECT 'shuffled one-pass dedup and split-state merge dedup pick the same survivors (all 1, twice):';
SELECT
    timeSeriesRateToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), shuffled), arrayMap(p -> p.2, shuffled))
    = (SELECT timeSeriesRateToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), all_pairs), arrayMap(p -> p.2, all_pairs)) FROM ts_pack_nan),
    timeSeriesResetsToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), shuffled), arrayMap(p -> p.2, shuffled))
    = (SELECT timeSeriesResetsToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), all_pairs), arrayMap(p -> p.2, all_pairs)) FROM ts_pack_nan)
FROM (SELECT arraySort(p -> cityHash64(p.1, p.2), all_pairs) AS shuffled FROM ts_pack_nan);

SELECT
    timeSeriesRateToGridMerge(0, 500, 100, 200)(st_rate) = (SELECT timeSeriesRateToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), all_pairs), arrayMap(p -> p.2, all_pairs)) FROM ts_pack_nan),
    timeSeriesResetsToGridMerge(0, 500, 100, 200)(st_resets) = (SELECT timeSeriesResetsToGrid(0, 500, 100, 200)(arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), all_pairs), arrayMap(p -> p.2, all_pairs)) FROM ts_pack_nan)
FROM
(
    SELECT
        initializeAggregation('timeSeriesRateToGridState(0, 500, 100, 200)', arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), inorder), arrayMap(p -> p.2, inorder)) AS st_rate,
        initializeAggregation('timeSeriesResetsToGridState(0, 500, 100, 200)', arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), inorder), arrayMap(p -> p.2, inorder)) AS st_resets
    FROM ts_pack_nan
    UNION ALL
    SELECT
        initializeAggregation('timeSeriesRateToGridState(0, 500, 100, 200)', arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), late), arrayMap(p -> p.2, late)),
        initializeAggregation('timeSeriesResetsToGridState(0, 500, 100, 200)', arrayMap(p -> toDateTime64(p.1, 3, 'UTC'), late), arrayMap(p -> p.2, late))
    FROM ts_pack_nan
)
SETTINGS max_threads = 1;

DROP TABLE ts_pack_nan;

-- Scalar path over two interleaved parts on a 200s-bucket grid: each part alone is in-order at 10s spacing, i.e. 20 samples per bucket - above the 12-sample pack threshold - so per-part streams really pack their buckets; the two-level merge combines packed partial states with interleaved timestamp ranges, and the single-threaded scan packs each bucket during the first part and unpack-repairs it when the second part's rows jump back at the part boundary.
DROP TABLE IF EXISTS ts_pack_one_part;
DROP TABLE IF EXISTS ts_pack_two_parts;
CREATE TABLE ts_pack_one_part (id UInt64, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_pack_two_parts (id UInt64, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
SYSTEM STOP MERGES ts_pack_two_parts;

-- 4 series, 400 samples each, 5s apart over (0, 2000]; a sawtooth counter with a period that is coprime to the bucket size.
INSERT INTO ts_pack_one_part SELECT number % 4, toDateTime64(5 + intDiv(number, 4) * 5, 3, 'UTC'), (intDiv(number, 4) % 23 * 10)::Float64 FROM numbers(1600);
INSERT INTO ts_pack_two_parts SELECT number % 4, toDateTime64(5 + intDiv(number, 4) * 5, 3, 'UTC'), (intDiv(number, 4) % 23 * 10)::Float64 FROM numbers(1600) WHERE intDiv(number, 4) % 2 = 0;
INSERT INTO ts_pack_two_parts SELECT number % 4, toDateTime64(5 + intDiv(number, 4) * 5, 3, 'UTC'), (intDiv(number, 4) % 23 * 10)::Float64 FROM numbers(1600) WHERE intDiv(number, 4) % 2 = 1;

SELECT 'single-threaded scan over interleaved parts equals the sorted part (all 1):';
SELECT a.id, a.rate = b.rate, a.changes = b.changes, a.deriv = b.deriv
FROM
(
    SELECT id, timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) AS rate, timeSeriesChangesToGrid(0, 2100, 200, 400)(timestamp, value) AS changes, timeSeriesDerivToGrid(0, 2100, 200, 400)(timestamp, value) AS deriv
    FROM ts_pack_two_parts GROUP BY id
    SETTINGS max_threads = 1
) AS a
INNER JOIN
(
    SELECT id, timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) AS rate, timeSeriesChangesToGrid(0, 2100, 200, 400)(timestamp, value) AS changes, timeSeriesDerivToGrid(0, 2100, 200, 400)(timestamp, value) AS deriv
    FROM ts_pack_one_part GROUP BY id
    SETTINGS max_threads = 1
) AS b USING (id)
ORDER BY a.id;

SELECT 'two-level parallel aggregation with tiny blocks equals the sorted part (all 1):';
SELECT a.id, a.rate = b.rate, a.changes = b.changes, a.deriv = b.deriv
FROM
(
    SELECT id, timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) AS rate, timeSeriesChangesToGrid(0, 2100, 200, 400)(timestamp, value) AS changes, timeSeriesDerivToGrid(0, 2100, 200, 400)(timestamp, value) AS deriv
    FROM ts_pack_two_parts GROUP BY id
    SETTINGS max_threads = 4, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_block_size = 7
) AS a
INNER JOIN
(
    SELECT id, timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) AS rate, timeSeriesChangesToGrid(0, 2100, 200, 400)(timestamp, value) AS changes, timeSeriesDerivToGrid(0, 2100, 200, 400)(timestamp, value) AS deriv
    FROM ts_pack_one_part GROUP BY id
    SETTINGS max_threads = 1
) AS b USING (id)
ORDER BY a.id;

-- `remote()` to two loopback shards serializes each shard's states in the plain wire format (packed buckets - each in-order part stream fills 20-sample buckets - decode into it), and the initiator deserializes, re-packs and merges two fully overlapping copies, whose deduplication must collapse every duplicate.
SELECT 'serialized-state merge of fully overlapping shards equals the sorted part (all 1):';
SELECT a.id, a.rate = b.rate, a.changes = b.changes, a.deriv = b.deriv
FROM
(
    SELECT id, timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) AS rate, timeSeriesChangesToGrid(0, 2100, 200, 400)(timestamp, value) AS changes, timeSeriesDerivToGrid(0, 2100, 200, 400)(timestamp, value) AS deriv
    FROM remote('127.0.0.{1,2}', currentDatabase(), ts_pack_two_parts) GROUP BY id
) AS a
INNER JOIN
(
    SELECT id, timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) AS rate, timeSeriesChangesToGrid(0, 2100, 200, 400)(timestamp, value) AS changes, timeSeriesDerivToGrid(0, 2100, 200, 400)(timestamp, value) AS deriv
    FROM ts_pack_one_part GROUP BY id
    SETTINGS max_threads = 1
) AS b USING (id)
ORDER BY a.id;

-- An `AggregatingMergeTree` roundtrip of states built from two series each (ids 0/2 then 1/3 per key): each half-series delivers 20 samples per 200s bucket, so the first series' buckets pack, the second series' backward jump unpack-repairs and re-packs them, and the INSERT serializes states whose buckets are packed (except the last, still raw) - decoding the blobs into the plain wire format; deserialization re-packs them, and both the query-time `-Merge` and the background merge behind `OPTIMIZE FINAL` combine overlapping packed states. Two inserts hold interleaved halves of each series, so the per-key states cover interleaved ranges.
SELECT 'AggregatingMergeTree roundtrip of half-packed states equals the direct result (all 1, twice):';
DROP TABLE IF EXISTS ts_pack_agg_states;
CREATE TABLE ts_pack_agg_states (k UInt8, st AggregateFunction(timeSeriesRateToGrid(0, 2100, 200, 400), DateTime64(3, 'UTC'), Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO ts_pack_agg_states SELECT id % 2, timeSeriesRateToGridState(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE toUnixTimestamp(timestamp) % 2 = 0 GROUP BY id % 2 SETTINGS max_threads = 1, max_block_size = 7;
INSERT INTO ts_pack_agg_states SELECT id % 2, timeSeriesRateToGridState(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE toUnixTimestamp(timestamp) % 2 = 1 GROUP BY id % 2 SETTINGS max_threads = 1, max_block_size = 7;
SELECT k, timeSeriesRateToGridMerge(0, 2100, 200, 400)(st) = (SELECT timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE id % 2 = k) FROM ts_pack_agg_states GROUP BY k ORDER BY k;
OPTIMIZE TABLE ts_pack_agg_states FINAL;
SELECT k, timeSeriesRateToGridMerge(0, 2100, 200, 400)(st) = (SELECT timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE id % 2 = k) FROM ts_pack_agg_states GROUP BY k ORDER BY k;
DROP TABLE ts_pack_agg_states;

-- The same roundtrip from purely in-order per-series states: a single-threaded scan of one series' half packs every bucket the write cursor leaves (20 samples per bucket) with no repairs involved, so the INSERT deterministically serializes genuinely packed buckets to disk, the read side re-packs them, and the query-time `-Merge` plus `OPTIMIZE FINAL` combine two packed states per key whose 10s-offset halves interleave.
SELECT 'AggregatingMergeTree roundtrip of packed in-order states equals the direct result (all 1, twice):';
DROP TABLE IF EXISTS ts_pack_agg_inorder;
CREATE TABLE ts_pack_agg_inorder (k UInt64, st AggregateFunction(timeSeriesRateToGrid(0, 2100, 200, 400), DateTime64(3, 'UTC'), Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO ts_pack_agg_inorder SELECT id AS k, timeSeriesRateToGridState(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE toUnixTimestamp(timestamp) % 2 = 0 GROUP BY id SETTINGS max_threads = 1;
INSERT INTO ts_pack_agg_inorder SELECT id AS k, timeSeriesRateToGridState(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE toUnixTimestamp(timestamp) % 2 = 1 GROUP BY id SETTINGS max_threads = 1;
SELECT k, timeSeriesRateToGridMerge(0, 2100, 200, 400)(st) = (SELECT timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE id = k) FROM ts_pack_agg_inorder GROUP BY k ORDER BY k;
OPTIMIZE TABLE ts_pack_agg_inorder FINAL;
SELECT k, timeSeriesRateToGridMerge(0, 2100, 200, 400)(st) = (SELECT timeSeriesRateToGrid(0, 2100, 200, 400)(timestamp, value) FROM ts_pack_one_part WHERE id = k) FROM ts_pack_agg_inorder GROUP BY k ORDER BY k;
DROP TABLE ts_pack_agg_inorder;

-- In-memory merges via `initializeAggregation`: two states over interleaved timestamp ranges (offset by 2s), a state merged with an identical copy of itself (full overlap), and the union built directly must all agree.
SELECT 'merge of interleaved packed states equals the direct union (1):';
SELECT timeSeriesRateToGridMerge(0, 1000, 100, 200)(st) = (SELECT timeSeriesRateToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), arrayConcat(ts_sec, arrayMap(t -> t + 2, ts_sec))), arrayConcat(vals, arrayMap(v -> v + 1, vals))) FROM ts_pack_gen)
FROM
(
    SELECT initializeAggregation('timeSeriesRateToGridState(0, 1000, 100, 200)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS st FROM ts_pack_gen
    UNION ALL
    SELECT initializeAggregation('timeSeriesRateToGridState(0, 1000, 100, 200)', arrayMap(x -> toDateTime64(x + 2, 3, 'UTC'), ts_sec), arrayMap(v -> v + 1, vals)) AS st FROM ts_pack_gen
)
SETTINGS max_threads = 1;

SELECT 'merge of two identical packed states equals one of them (1):';
SELECT timeSeriesIncreaseToGridMerge(0, 1000, 100, 200)(st) = (SELECT timeSeriesIncreaseToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) FROM ts_pack_gen)
FROM
(
    SELECT initializeAggregation('timeSeriesIncreaseToGridState(0, 1000, 100, 200)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS st FROM ts_pack_gen
    UNION ALL
    SELECT initializeAggregation('timeSeriesIncreaseToGridState(0, 1000, 100, 200)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_sec), vals) AS st FROM ts_pack_gen
)
SETTINGS max_threads = 1;

-- The other supported argument types: `DateTime` timestamps (32-bit) and `Float32` values take the same packed paths.
SELECT 'DateTime timestamps with Float32 values, shuffled equals sorted (1):';
SELECT
    timeSeriesRateToGrid(0, 1000, 100, 200)(arrayMap(p -> toDateTime(p.1, 'UTC'), shuffled), CAST(arrayMap(p -> p.2, shuffled), 'Array(Float32)'))
    = timeSeriesRateToGrid(0, 1000, 100, 200)(arrayMap(x -> toDateTime(x, 'UTC'), ts_sec), CAST(vals, 'Array(Float32)'))
FROM
(
    SELECT ts_sec, vals, arraySort(p -> cityHash64(p.1, p.2), arrayZip(ts_sec, vals)) AS shuffled
    FROM ts_pack_expected
);

-- A single-point grid keeps the whole series in one bucket that the write cursor never leaves: the bucket packs itself at the self-pack threshold (1024 samples) and the remaining in-order samples take the O(1) packed append; the value jump at index 600 exercises the huge-delta tokens under `DateTime64(9)` timestamps.
SELECT 'single-bucket self-pack at 1100 samples, shuffled equals sorted (1):';
WITH
    arrayMap(i -> toInt64(1000000 + i), range(1100)) AS ts_sec9,
    arrayMap(i -> if(i = 600, 1e15, i * 0.001), range(1100)) AS vals9,
    arraySort(p -> cityHash64(p.1, p.2), arrayZip(ts_sec9, vals9)) AS shuffled9
SELECT
    timeSeriesIncreaseToGrid(toDateTime64(2000000, 9, 'UTC'), toDateTime64(2000000, 9, 'UTC'), 1, 1500000)(arrayMap(x -> toDateTime64(x, 9, 'UTC'), ts_sec9), vals9)
    = timeSeriesIncreaseToGrid(toDateTime64(2000000, 9, 'UTC'), toDateTime64(2000000, 9, 'UTC'), 1, 1500000)(arrayMap(p -> toDateTime64(p.1, 9, 'UTC'), shuffled9), arrayMap(p -> p.2, shuffled9));

DROP TABLE ts_pack_one_part;
DROP TABLE ts_pack_two_parts;
DROP TABLE ts_pack_expected;
DROP TABLE ts_pack_late;
DROP TABLE ts_pack_gen;
