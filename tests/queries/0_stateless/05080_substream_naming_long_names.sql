-- Namespaces make stream names longer, so they reach `max_file_name_length` sooner and the hashing
-- fallback has to keep them resolvable. The limit here is small enough that only the new scheme
-- crosses it.

DROP TABLE IF EXISTS t_long_basic;
DROP TABLE IF EXISTS t_long_ns;

CREATE TABLE t_long_basic (
    a_quite_long_column_name_for_testing Array(Array(Tuple(some_long_element_name Array(Nullable(String))))),
    j JSON(`a_long_json_path_name_here` Array(Nullable(Int64)))
) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             replace_long_file_name_to_hash = 1, max_file_name_length = 79,
             substream_naming_version = 'basic';

CREATE TABLE t_long_ns (
    a_quite_long_column_name_for_testing Array(Array(Tuple(some_long_element_name Array(Nullable(String))))),
    j JSON(`a_long_json_path_name_here` Array(Nullable(Int64)))
) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             replace_long_file_name_to_hash = 1, max_file_name_length = 79,
             substream_naming_version = 'namespaced';

INSERT INTO t_long_basic VALUES ([[(['ab',NULL])]], '{"a_long_json_path_name_here":[1,null]}');
INSERT INTO t_long_ns VALUES ([[(['ab',NULL])]], '{"a_long_json_path_name_here":[1,null]}');

SELECT 'values round trip';
SELECT a_quite_long_column_name_for_testing, j FROM t_long_basic;
SELECT a_quite_long_column_name_for_testing, j FROM t_long_ns;

SELECT 'subcolumns round trip';
SELECT a_quite_long_column_name_for_testing.size0, a_quite_long_column_name_for_testing.some_long_element_name FROM t_long_basic;
SELECT a_quite_long_column_name_for_testing.size0, a_quite_long_column_name_for_testing.some_long_element_name FROM t_long_ns;
SELECT j.a_long_json_path_name_here FROM t_long_basic;
SELECT j.a_long_json_path_name_here FROM t_long_ns;

-- No name may exceed the limit, and the new scheme has to hash some that the old one keeps intact.
-- Only whether anything was hashed is asserted: the JSON shared data substream count is randomized.
SELECT 'names over the limit, then any hashed name';
SELECT countIf(length(f) > 79), countIf(match(f, '^[0-9a-f]{32}$')) > 0 FROM
    (SELECT arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_long_basic' AND active);
SELECT countIf(length(f) > 79), countIf(match(f, '^[0-9a-f]{32}$')) > 0 FROM
    (SELECT arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_long_ns' AND active);

DROP TABLE t_long_basic;
DROP TABLE t_long_ns;
