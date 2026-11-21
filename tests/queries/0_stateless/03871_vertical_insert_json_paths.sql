-- Test: JSON typed paths, dynamic paths, and aliases in ORDER BY work with vertical insert.
SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_vi_json_paths;

CREATE TABLE t_vi_json_paths
(
    id UInt64,
    json JSON(a UInt32, b String, c String, arr Array(UInt32)),
    note String,
    b_alias String ALIAS json.b,
    c_alias String ALIAS json.c
)
ENGINE = MergeTree
ORDER BY (json.b, json.c, id)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_json_paths (id, json, note) VALUES
    (1, '{"a":2,"b":"bb","arr":[1,2],"c":"x"}'::JSON(a UInt32, b String, c String, arr Array(UInt32)), 'n1'),
    (2, '{"a":1,"b":"aa","arr":[3],"c":"y","extra":42}'::JSON(a UInt32, b String, c String, arr Array(UInt32)), 'n2'),
    (3, '{"a":1,"b":"ab","arr":[4,5],"c":"x"}'::JSON(a UInt32, b String, c String, arr Array(UInt32)), 'n3');

SELECT id, json.a, b_alias, c_alias, note
FROM t_vi_json_paths
ORDER BY b_alias, c_alias, id;

DROP TABLE t_vi_json_paths;
