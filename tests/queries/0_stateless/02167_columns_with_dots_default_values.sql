DROP TABLE IF EXISTS test_nested_default;

CREATE TABLE test_nested_default
(
    `id` String,
    `with_dot.str` String,
    `with_dot.array` Array(String)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_nested_default(`id`, `with_dot.array`) VALUES('id', ['str1', 'str2']);
SELECT * FROM test_nested_default;

DROP TABLE test_nested_default;
