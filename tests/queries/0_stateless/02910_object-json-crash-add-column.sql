DROP TABLE IF EXISTS test02910;

CREATE TABLE test02910
(
	i Int8,
	jString String
) ENGINE = MergeTree
ORDER BY i;

INSERT INTO test02910 (i, jString) SELECT 1, '{"a":"123"}';
ALTER TABLE test02910 ADD COLUMN j2 JSON default jString;  -- { serverError SUPPORT_IS_DISABLED }

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

ALTER TABLE test02910_second ADD COLUMN `tags_json` JSON; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE IF EXISTS test02910;
DROP TABLE IF EXISTS test02910_second;
