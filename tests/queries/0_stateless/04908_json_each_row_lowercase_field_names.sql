-- The exact scenario from https://github.com/ClickHouse/ClickHouse/issues/92851:
-- the table columns are `Hello` and `World`, while the input data spells the field
-- names in lowercase. With the default `input_format_column_name_matching_mode = 'auto'`
-- the fields are matched case-insensitively, so the values are inserted instead of
-- silently becoming empty strings.

DROP TABLE IF EXISTS test;

CREATE TABLE test (Hello String, World String) ENGINE = Memory;

INSERT INTO test FORMAT JSONEachRow {"hello": "hi!", "world": "universe"}
;

SELECT * FROM test;

DROP TABLE test;
