DROP TABLE IF EXISTS json_bf_disjunction_reads;
CREATE TABLE json_bf_disjunction_reads
(
    j JSON(a UInt64, b UInt64, c UInt64, common String),
    INDEX bf j TYPE jsonbf_v1() GRANULARITY 1,
    INDEX bf_a j TYPE jsonbf_v1(include_paths = ['a']) GRANULARITY 1,
    INDEX bf_b j TYPE jsonbf_v1(include_paths = ['b']) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;
INSERT INTO json_bf_disjunction_reads
SELECT ('{"a":' || toString(number) || ',"b":' || toString(number + 10000)
    || ',"c":' || toString(number % 128) || ',"common":"yes"}')::JSON(a UInt64, b UInt64, c UInt64, common String)
FROM numbers(2048);

SET force_data_skipping_indices = 'bf';
SET use_skip_indexes_on_data_read = 0;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 10 OR j.b = 12000;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 10000 OR j.b = 20000;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 2000 AND j.b = 12000;
SELECT count() FROM json_bf_disjunction_reads WHERE (j.a = 10 OR j.b = 12000) AND j.c = 80;
SELECT count() FROM json_bf_disjunction_reads WHERE (j.a % 7 = 0 OR j.b = 12000) AND j.c = 80;
SELECT count() FROM json_bf_disjunction_reads WHERE j.common = 'yes' AND NOT (j.a = 10 OR j.b = 10020);

SET use_skip_indexes_on_data_read = 1;
SET max_threads = 8;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 10 OR j.b = 12000;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 10000 OR j.b = 20000;
SELECT count() FROM json_bf_disjunction_reads WHERE (j.a = 10 OR j.b = 12000) AND j.c = 80;
SELECT count() FROM json_bf_disjunction_reads WHERE (j.a % 7 = 0 OR j.b = 12000) AND j.c = 80;
SELECT count() FROM json_bf_disjunction_reads WHERE j.common = 'yes' AND NOT (j.a = 10 OR j.b = 10020);

SET use_skip_indexes_for_disjunctions = 0;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 10 OR j.b = 12000;
SELECT count() FROM json_bf_disjunction_reads WHERE (j.a % 7 = 0 OR j.b = 12000) AND j.c = 80;
ALTER TABLE json_bf_disjunction_reads UPDATE
    j = '{"a":9999,"b":10000,"c":0,"common":"yes"}'::JSON
WHERE j.a = 0 SETTINGS mutations_sync = 2;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 9999;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 0;
DETACH TABLE json_bf_disjunction_reads;
ATTACH TABLE json_bf_disjunction_reads;
SELECT count() FROM json_bf_disjunction_reads WHERE j.a = 9999 OR j.b = 12000;
DROP TABLE json_bf_disjunction_reads;
