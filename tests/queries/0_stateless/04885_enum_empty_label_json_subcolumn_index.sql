-- Tags: no-fasttest
-- Tag no-fasttest: fasttest builds with ENABLE_LIBRARIES=0, which leaves the `JSON` type no JSON parser
-- A `JSONAllPaths` index may skip a granule that lacks the path only when the compared constant
-- differs from the value a missing path produces. An `Enum` constant carries its labels in its own
-- type and the comparison uses the label, so an empty label is that value and nothing may be skipped.
-- The same holds for a `FixedString` operand on either side: the comparison ignores trailing zero
-- padding, and a `FixedString` reads its full width of zero bytes where the path is missing, so an
-- all-zero constant is that value too.

DROP TABLE IF EXISTS t_json_bf;
DROP TABLE IF EXISTS t_json_tokenbf;
DROP TABLE IF EXISTS t_json_tuple;

CREATE TABLE t_json_bf (id UInt64, data JSON, INDEX idx JSONAllPaths(data) TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_json_tokenbf (id UInt64, data JSON, INDEX idx JSONAllPaths(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_json_bf      VALUES (1, '{"alpha":"x"}'), (2, '{"beta":"y"}'), (3, '{"alpha":""}'), (4, '{}');
INSERT INTO t_json_tokenbf VALUES (1, '{"alpha":"x"}'), (2, '{"beta":"y"}'), (3, '{"alpha":""}'), (4, '{}');

-- Ids 2 and 4 carry no `alpha` path, so the non-nullable `String` subcolumn reads as '' and they match.
SELECT arraySort(groupArray(id)) FROM t_json_bf      WHERE data.alpha::String = CAST('', 'Enum8('''' = 3)');
SELECT arraySort(groupArray(id)) FROM t_json_tokenbf WHERE data.alpha::String = CAST('', 'Enum8('''' = 3)');

-- A `Variant` or `Dynamic` constant reports its own declared type while yielding the active
-- alternative's value, so an `Enum` alternative cannot be told apart and nothing may be skipped.
SELECT arraySort(groupArray(id)) FROM t_json_bf WHERE data.alpha::String = CAST('', 'Variant(Enum8('''' = 3))');
SELECT arraySort(groupArray(id)) FROM t_json_bf WHERE data.alpha::String = CAST(CAST('', 'Enum8('''' = 3)'), 'Dynamic');

-- A tuple comparison carries each element's own type, so a `Nullable` source reaches the same
-- decision still wrapped. Only `WHERE` is split into per-element comparisons without the analyzer,
-- so `PREWHERE` is where a tuple reaches index analysis whole.
SELECT arraySort(groupArray(id)) FROM t_json_bf PREWHERE (data.alpha::String, id) = (CAST('', 'Nullable(Enum8('''' = 3))'), 2) SETTINGS enable_analyzer = 0;

-- A non-empty label on that same wrapped route differs from the default, so the index stays usable.
SELECT arraySort(groupArray(id)) FROM t_json_bf PREWHERE (data.alpha::String, id) = (CAST('7', 'Nullable(Enum8(''7'' = 3))'), 2) SETTINGS enable_analyzer = 0, force_data_skipping_indices = 'idx';

SELECT arraySort(groupArray(id)) FROM t_json_bf WHERE data.alpha::String = '';

-- A non-empty label differs from the default, so each index stays usable and keeps pruning;
-- `force_data_skipping_indices` throws when it does not.
SELECT arraySort(groupArray(id)) FROM t_json_bf      WHERE data.alpha::String = CAST('7', 'Enum8(''7'' = 3)') SETTINGS force_data_skipping_indices = 'idx';
SELECT arraySort(groupArray(id)) FROM t_json_tokenbf WHERE data.alpha::String = CAST('7', 'Enum8(''7'' = 3)') SETTINGS force_data_skipping_indices = 'idx';

-- A `Tuple` key expression takes the whole constant through one conversion, and that conversion
-- recurses into the elements without their own types, so a nested empty `Enum` label is lost.
CREATE TABLE t_json_tuple (id UInt64, data JSON, INDEX idx JSONAllPaths(data) TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_json_tuple VALUES (1, '{"alpha":"(''x'')"}'), (2, '{"beta":"y"}'), (3, '{"alpha":"('''')"}'), (4, '{}');

-- Ids 2 and 4 carry no `alpha` path, so the non-nullable `Tuple(String)` reads as `('')` and they match.
SELECT arraySort(groupArray(id)) FROM t_json_tuple WHERE data.alpha::Tuple(String) = tuple(CAST('', 'Enum8('''' = 3)'));

-- A nested `Dynamic` names no alternative in its type, so it is the one carrier whose `Enum` stays
-- invisible even where the element types are walked.
SELECT arraySort(groupArray(id)) FROM t_json_tuple WHERE data.alpha::Tuple(String) = tuple(CAST(CAST('', 'Enum8('''' = 3)'), 'Dynamic'));

-- A tuple constant holding no `Enum` loses nothing in that conversion, so the index stays usable.
SELECT arraySort(groupArray(id)) FROM t_json_tuple WHERE data.alpha::Tuple(String) = tuple('x') SETTINGS force_data_skipping_indices = 'idx';

-- An all-zero `FixedString` constant equals the empty `String` a missing path reads, so ids 2 and 4
-- match and nothing may be skipped. Its `Nullable` and `LowCardinality` wrappers compare the same way.
SELECT arraySort(groupArray(id)) FROM t_json_bf      WHERE data.alpha::String = CAST('', 'FixedString(4)');
SELECT arraySort(groupArray(id)) FROM t_json_tokenbf WHERE data.alpha::String = CAST('', 'FixedString(4)');
SELECT arraySort(groupArray(id)) FROM t_json_bf      WHERE data.alpha::String = CAST('', 'LowCardinality(FixedString(4))');

-- A non-empty constant differs from that value, so the index stays usable and keeps pruning.
SELECT arraySort(groupArray(id)) FROM t_json_bf WHERE data.alpha::String = CAST('x', 'FixedString(1)') SETTINGS force_data_skipping_indices = 'idx';

-- A `FixedString` key expression reads its full width of zero bytes where the path is missing, which
-- the comparison finds equal to an all-zero constant of any width.
SELECT arraySort(groupArray(id)) FROM t_json_bf WHERE data.alpha::FixedString(4) = CAST('', 'FixedString(4)');
SELECT arraySort(groupArray(id)) FROM t_json_bf WHERE data.alpha::FixedString(4) = CAST('', 'FixedString(6)');

-- Each element of a composite reads its own default, so a nested `FixedString` carries the same value.
SELECT arraySort(groupArray(id)) FROM t_json_tuple WHERE data.alpha::Tuple(FixedString(4)) = tuple('');

-- A nested non-empty `Enum` label differs from that value, so the index prunes here as well.
SELECT arraySort(groupArray(id)) FROM t_json_tuple WHERE data.alpha::Tuple(String) = tuple(CAST('7', 'Enum8(''7'' = 3)')) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_json_bf;
DROP TABLE t_json_tokenbf;
DROP TABLE t_json_tuple;
