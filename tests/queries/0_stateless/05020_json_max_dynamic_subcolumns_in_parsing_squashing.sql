-- `max_dynamic_subcolumns_in_json_type_parsing` must cap dynamic paths regardless of how the inserted
-- data was split into blocks. With `max_block_size = 1` the first block holds a single path and spills
-- nothing into shared data, which is the case where squashing used to widen the cap back to the type's.

DROP TABLE IF EXISTS t_json_parsing_cap_squashing;
CREATE TABLE t_json_parsing_cap_squashing (id UInt64, json JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_parsing_cap_squashing
SELECT number, multiIf(number = 0, '{"a" : 42}', number = 1, '{"b" : 42}', number = 2, '{"c" : 42}', '{"d" : 42}')
FROM numbers(4)
SETTINGS max_block_size = 1, max_dynamic_subcolumns_in_json_type_parsing = 2;

SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM t_json_parsing_cap_squashing ORDER BY id;

DROP TABLE t_json_parsing_cap_squashing;
