SET allow_experimental_object_type=1;

DROP TABLE IF EXISTS test02910;

CREATE TABLE test02910
(
	i Int8,
	jString String
) ENGINE = MergeTree
ORDER BY i;

INSERT INTO test02910 (i, jString) SELECT 1, '{"a":"123"}';

ALTER TABLE test02910 ADD COLUMN j2 Tuple(Object('json')) DEFAULT jString;  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test02910 ADD COLUMN j2 Tuple(Float64, Object('json'));  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test02910 ADD COLUMN j2 Tuple(Array(Tuple(Object('json')))) DEFAULT jString;  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test02910 ADD COLUMN j2 Object('json') default jString;  -- { serverError SUPPORT_IS_DISABLED }

-- If we would allow adding a column with dynamic subcolumns the subsequent select would crash the server.
-- SELECT * FROM test02910;

DROP TABLE IF EXISTS test02910_second;

CREATE TABLE test02910_second
(
    `Id1` String,
    `Id2` String,
    `timestamp` DateTime64(6),
    `tags` Array(String),
)
ENGINE = MergeTree
PRIMARY KEY (Id1, Id2)
ORDER BY (Id1, Id2, timestamp)
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO test02910_second SELECT number, number, '2023-10-28 11:11:11.11111', [] FROM numbers(10);
INSERT INTO test02910_second SELECT number, number, '2023-10-28 11:11:11.11111', ['a'] FROM numbers(10);
INSERT INTO test02910_second SELECT number, number, '2023-10-28 11:11:11.11111', ['b'] FROM numbers(10);
INSERT INTO test02910_second SELECT number, number, '2023-10-28 11:11:11.11111', ['c', 'd'] FROM numbers(10);
INSERT INTO test02910_second SELECT number, number, '2023-10-28 11:11:11.11111', [] FROM numbers(10);

ALTER TABLE test02910_second ADD COLUMN `tags_json` Tuple(Object('json')) DEFAULT jString;  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test02910_second ADD COLUMN `tags_json` Tuple(Float64, Object('json'));  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test02910_second ADD COLUMN `tags_json` Tuple(Array(Tuple(Object('json')))) DEFAULT jString;  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE test02910_second ADD COLUMN `tags_json` Object('json'); -- { serverError SUPPORT_IS_DISABLED }

-- If we would allow adding a column with dynamic subcolumns the subsequent select would crash the server.
-- SELECT * FROM test02910;

DROP TABLE IF EXISTS test02910;
DROP TABLE IF EXISTS test02910_second;
