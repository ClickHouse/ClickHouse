-- { echo }

-- With a reversed (DESC) key column and non-adaptive granularity (`index_granularity_bytes = 0`,
-- so the part has no final mark), the last granule's range has no upper mark. The unknown side
-- extends toward the end of the part, which is the smaller values for a reversed column, so the
-- substituted bound must be -Inf on the value-ascending left side. Building it with the wrong
-- direction made the primary key index silently prune matching granules.

DROP TABLE IF EXISTS test_reverse_no_final_mark;
CREATE TABLE test_reverse_no_final_mark (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY ts DESC
SETTINGS index_granularity = 1, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO test_reverse_no_final_mark SELECT toDateTime('2026-03-30 00:00:00', 'UTC') + number * 3600 FROM numbers(24);

SELECT count() FROM test_reverse_no_final_mark WHERE ts > toDateTime('2026-03-30 12:00:00', 'UTC');
SELECT count() FROM test_reverse_no_final_mark WHERE ts > toDateTime('2026-03-30 12:00:00', 'UTC') SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_no_final_mark WHERE ts < toDateTime('2026-03-30 12:00:00', 'UTC');
SELECT count() FROM test_reverse_no_final_mark WHERE ts < toDateTime('2026-03-30 12:00:00', 'UTC') SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_no_final_mark WHERE ts = toDateTime('2026-03-30 06:00:00', 'UTC');
SELECT count() FROM test_reverse_no_final_mark WHERE ts = toDateTime('2026-03-30 06:00:00', 'UTC') SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_no_final_mark WHERE ts IN (toDateTime('2026-03-30 06:00:00', 'UTC'), toDateTime('2026-03-30 12:00:00', 'UTC'));
SELECT count() FROM test_reverse_no_final_mark WHERE ts IN (toDateTime('2026-03-30 06:00:00', 'UTC'), toDateTime('2026-03-30 12:00:00', 'UTC')) SETTINGS use_primary_key = 0;

-- The value of the very last granule (the smallest value in a reversed part) must be found.
SELECT count() FROM test_reverse_no_final_mark WHERE ts = toDateTime('2026-03-30 00:00:00', 'UTC');
SELECT count() FROM test_reverse_no_final_mark WHERE ts = toDateTime('2026-03-30 00:00:00', 'UTC') SETTINGS use_primary_key = 0;

-- Both analysis paths must agree.
SELECT count() FROM test_reverse_no_final_mark WHERE ts > toDateTime('2026-03-30 12:00:00', 'UTC') SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM test_reverse_no_final_mark WHERE ts = toDateTime('2026-03-30 06:00:00', 'UTC') SETTINGS use_lightweight_primary_key_index_analysis = 0;

DROP TABLE test_reverse_no_final_mark;

-- A multi-expression reversed key: derived leading columns receive the same malformed range;
-- in debug builds this tripped `Invalid binary search result in MergeTreeSetIndex`.
DROP TABLE IF EXISTS test_reverse_multi;
CREATE TABLE test_reverse_multi (ts DateTime('UTC')) ENGINE = MergeTree
ORDER BY (toYYYYMM(ts) DESC, toDate(ts) DESC, ts DESC)
SETTINGS index_granularity = 1, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO test_reverse_multi SELECT toDateTime('2026-03-30 00:00:00', 'UTC') + toIntervalDay(intDiv(number, 4)) + toIntervalHour((number % 4) * 6) FROM numbers(24);

SELECT count() FROM test_reverse_multi WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC'));
SELECT count() FROM test_reverse_multi WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS use_primary_key = 0;
SELECT count() FROM test_reverse_multi WHERE ts IN (toDateTime('2026-03-31 06:00:00', 'UTC'), toDateTime('2026-04-02 12:00:00', 'UTC')) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM test_reverse_multi WHERE ts = toDateTime('2026-03-30 00:00:00', 'UTC');
SELECT count() FROM test_reverse_multi WHERE ts = toDateTime('2026-03-30 00:00:00', 'UTC') SETTINGS use_primary_key = 0;

DROP TABLE test_reverse_multi;
