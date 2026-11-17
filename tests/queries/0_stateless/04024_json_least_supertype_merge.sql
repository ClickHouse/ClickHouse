-- Tags: no-fasttest

-- Test for getLeastSupertype of JSON types with different parameters.
-- Verifies that two JSON columns with different parameters merge to a JSON
-- (not Variant) in StorageMerge, fixing UNKNOWN_IDENTIFIER for ALIAS columns
-- referencing JSON sub-paths.
-- https://github.com/ClickHouse/ClickHouse/issues/97812

SET enable_analyzer = 1;

DROP TABLE IF EXISTS json_merge_t1;
DROP TABLE IF EXISTS json_merge_t2;
DROP TABLE IF EXISTS json_merge_m;

-- Basic reproduction: different SKIP fields with ALIAS referencing JSON sub-path.
CREATE TABLE json_merge_t1
(
    `json` JSON(SKIP some_field),
    `display_name` String ALIAS json.display_name::String
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE json_merge_t2
(
    `json` JSON(SKIP another_field)
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE json_merge_m ENGINE = Merge(currentDatabase(), 'json_merge_t.*');

-- The merged json column should be JSON, not Variant(JSON(...), JSON(...)).
SELECT 'basic: merged type';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'json_merge_m' AND name = 'json';

-- This used to fail with UNKNOWN_IDENTIFIER because json became Variant.
SELECT 'basic: count';
SELECT count() FROM json_merge_m;

-- Verify JSON subcolumn reads work through Merge when ALIAS columns are present.
INSERT INTO json_merge_t1 VALUES ('{"display_name": "Alice", "x": 1}');
INSERT INTO json_merge_t2 VALUES ('{"display_name": "Bob", "x": 2}');

SELECT 'basic: subcolumn read';
SELECT json.display_name::String AS dn, json.x::Int64 AS x FROM json_merge_m ORDER BY x;

-- Verify reading the ALIAS column itself works through Merge.
SELECT 'basic: alias read';
SELECT display_name FROM json_merge_m ORDER BY display_name;

DROP TABLE json_merge_m;
DROP TABLE json_merge_t1;
DROP TABLE json_merge_t2;

-- Test: different typed paths — intersection with type promotion.
DROP TABLE IF EXISTS json_typed_t1;
DROP TABLE IF EXISTS json_typed_t2;
DROP TABLE IF EXISTS json_typed_m;

CREATE TABLE json_typed_t1 (json JSON(a UInt32, b String)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_typed_t2 (json JSON(a UInt64, c Float64)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_typed_m ENGINE = Merge(currentDatabase(), 'json_typed_t.*');

-- Supertype should be JSON(a UInt64): b and c are dropped (not in both), a is promoted UInt32+UInt64=UInt64.
SELECT 'typed paths: merged type';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'json_typed_m' AND name = 'json';

SELECT 'typed paths: count';
SELECT count() FROM json_typed_m;

DROP TABLE json_typed_m;
DROP TABLE json_typed_t1;
DROP TABLE json_typed_t2;

-- Test: different max_dynamic_paths and max_dynamic_types — take max.
DROP TABLE IF EXISTS json_limits_t1;
DROP TABLE IF EXISTS json_limits_t2;
DROP TABLE IF EXISTS json_limits_m;

CREATE TABLE json_limits_t1 (json JSON(max_dynamic_paths=100, max_dynamic_types=10)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_limits_t2 (json JSON(max_dynamic_paths=200, max_dynamic_types=20)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_limits_m ENGINE = Merge(currentDatabase(), 'json_limits_t.*');

SELECT 'limits: merged type';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'json_limits_m' AND name = 'json';

SELECT 'limits: count';
SELECT count() FROM json_limits_m;

DROP TABLE json_limits_m;
DROP TABLE json_limits_t1;
DROP TABLE json_limits_t2;

-- Test: common SKIP paths are kept, uncommon are removed.
DROP TABLE IF EXISTS json_skip_t1;
DROP TABLE IF EXISTS json_skip_t2;
DROP TABLE IF EXISTS json_skip_m;

CREATE TABLE json_skip_t1 (json JSON(SKIP a, SKIP common)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_skip_t2 (json JSON(SKIP b, SKIP common)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_skip_m ENGINE = Merge(currentDatabase(), 'json_skip_t.*');

-- Supertype should have SKIP common (intersection), not SKIP a or SKIP b.
SELECT 'skip: merged type';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'json_skip_m' AND name = 'json';

DROP TABLE json_skip_m;
DROP TABLE json_skip_t1;
DROP TABLE json_skip_t2;

-- Test: common SKIP REGEXP strings are kept, uncommon are removed.
DROP TABLE IF EXISTS json_regexp_t1;
DROP TABLE IF EXISTS json_regexp_t2;
DROP TABLE IF EXISTS json_regexp_m;

CREATE TABLE json_regexp_t1 (json JSON(SKIP REGEXP 'internal_.*', SKIP REGEXP 'debug_.*')) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_regexp_t2 (json JSON(SKIP REGEXP 'internal_.*', SKIP REGEXP 'temp_.*')) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_regexp_m ENGINE = Merge(currentDatabase(), 'json_regexp_t.*');

-- Supertype should have SKIP REGEXP 'internal_.*' only.
SELECT 'regexp: merged type';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'json_regexp_m' AND name = 'json';

DROP TABLE json_regexp_m;
DROP TABLE json_regexp_t1;
DROP TABLE json_regexp_t2;

-- Test: old analyzer also works.
DROP TABLE IF EXISTS json_old_t1;
DROP TABLE IF EXISTS json_old_t2;
DROP TABLE IF EXISTS json_old_m;

CREATE TABLE json_old_t1 (json JSON(SKIP x), display_name String ALIAS json.display_name::String) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_old_t2 (json JSON(SKIP y)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE json_old_m ENGINE = Merge(currentDatabase(), 'json_old_t.*');

SELECT 'old analyzer';
SELECT count() FROM json_old_m SETTINGS enable_analyzer = 0;
SELECT count() FROM json_old_m SETTINGS enable_analyzer = 1;

DROP TABLE json_old_m;
DROP TABLE json_old_t1;
DROP TABLE json_old_t2;
