-- A SHARED Map path written with the bucketed Map serialization (randomized in CI), read alone, with another path and
-- with the whole column, in the three part variants, against the same reads on the type without SHARED.

DROP TABLE IF EXISTS mb_w;
DROP TABLE IF EXISTS mb_cm;
DROP TABLE IF EXISTS mb_cn;
DROP TABLE IF EXISTS mb_plain;
CREATE TABLE mb_w (x UInt64, json JSON(SHARED arr Array(UInt8), SHARED m Map(String, UInt64))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', max_buckets_in_map = 4;
CREATE TABLE mb_cm (x UInt64, json JSON(SHARED arr Array(UInt8), SHARED m Map(String, UInt64))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, write_marks_for_substreams_in_compact_parts = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', max_buckets_in_map = 4;
CREATE TABLE mb_cn (x UInt64, json JSON(SHARED arr Array(UInt8), SHARED m Map(String, UInt64))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, write_marks_for_substreams_in_compact_parts = 0, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', max_buckets_in_map = 4;
CREATE TABLE mb_plain (x UInt64, json JSON(arr Array(UInt8), m Map(String, UInt64))) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', max_buckets_in_map = 4;
INSERT INTO mb_w SELECT number, concat('{"arr":', toString(range(number % 3)), ',"m":{"k', toString(number % 2), '":', toString(number), ',"q', toString(number % 5), '":1}}') FROM numbers(20);
INSERT INTO mb_cm SELECT * FROM mb_w;
INSERT INTO mb_cn SELECT * FROM mb_w;
INSERT INTO mb_plain SELECT * FROM mb_w;
SELECT 'plain', sum(json.arr.size0), sum(length(json.m)), sum(json.m['k0']), count(json) FROM mb_plain SETTINGS max_threads = 1;
SELECT 'plain', sum(json.arr.size0), count(json) FROM mb_plain SETTINGS max_threads = 1;
SELECT 'wide', sum(json.arr.size0), sum(length(json.m)), sum(json.m['k0']), count(json) FROM mb_w SETTINGS max_threads = 1;
SELECT 'wide', sum(json.arr.size0), count(json) FROM mb_w SETTINGS max_threads = 1;
SELECT 'wide', count(json), sum(length(json.m)) FROM mb_w SETTINGS max_threads = 1;
SELECT 'compact marks', sum(json.arr.size0), sum(length(json.m)), sum(json.m['k0']), count(json) FROM mb_cm SETTINGS max_threads = 1;
SELECT 'compact marks', sum(json.arr.size0), count(json) FROM mb_cm SETTINGS max_threads = 1;
SELECT 'compact no marks', sum(json.arr.size0), sum(length(json.m)), sum(json.m['k0']), count(json) FROM mb_cn SETTINGS max_threads = 1;
SELECT 'compact no marks', sum(json.arr.size0), count(json) FROM mb_cn SETTINGS max_threads = 1;
SELECT 'equal', (SELECT groupArray((x, json)) FROM (SELECT * FROM mb_w ORDER BY x)) = (SELECT groupArray((x, json)) FROM (SELECT * FROM mb_plain ORDER BY x)), (SELECT groupArray((x, json.m)) FROM (SELECT * FROM mb_cn ORDER BY x)) = (SELECT groupArray((x, json.m)) FROM (SELECT * FROM mb_plain ORDER BY x));
DROP TABLE mb_w;
DROP TABLE mb_cm;
DROP TABLE mb_cn;
DROP TABLE mb_plain;
