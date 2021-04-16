DROP TABLE IF EXISTS test_wide_nested;

CREATE TABLE test_wide_nested
(
    `id` Int,
    `info.id` Array(Int),
    `info.name` Array(String),
    `info.age` Array(Int)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

set mutations_sync = 1;

INSERT INTO test_wide_nested SELECT number, [number,number + 1], ['aa','bb'], [number,number * 2] FROM numbers(5);

alter table test_wide_nested update `info.id` = [100,200] where id = 1;
select * from test_wide_nested where id = 1 order by id;

alter table test_wide_nested update `info.id` = [100,200,300], `info.age` = [10,20,30], `info.name` = ['a','b','c'] where id = 2;
select * from test_wide_nested;

alter table test_wide_nested update `info.id` = [100,200,300], `info.age` = `info.id`, `info.name` = ['a','b','c'] where id = 2;
select * from test_wide_nested;

alter table test_wide_nested update `info.id` = [100,200], `info.age`=[68,72] where id = 3;
alter table test_wide_nested update `info.id` = `info.age` where id = 3;
select * from test_wide_nested;

alter table test_wide_nested update `info.id` = [100,200], `info.age` = [10,20,30], `info.name` = ['a','b','c']  where id = 0; -- { serverError 341 }

-- Recreate table, because KILL MUTATION is not suitable for parallel tests execution.
DROP TABLE test_wide_nested;

CREATE TABLE test_wide_nested
(
    `id` Int,
    `info.id` Array(Int),
    `info.name` Array(String),
    `info.age` Array(Int)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_wide_nested SELECT number, [number,number + 1], ['aa','bb'], [number,number * 2] FROM numbers(5);

alter table test_wide_nested update `info.id` = [100,200,300], `info.age` = [10,20,30] where id = 1; -- { serverError 341 }

DROP TABLE test_wide_nested;

DROP TABLE IF EXISTS test_wide_not_nested;

CREATE TABLE test_wide_not_nested
(
  `id` Int,
  `info.id` Int,
  `info.name` String,
  `info.age` Int
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_wide_not_nested SELECT number, number, 'aa', number * 2 FROM numbers(5);
ALTER TABLE test_wide_not_nested UPDATE `info.name` = 'bb' WHERE id = 1;
SELECT * FROM test_wide_not_nested ORDER BY id;

DROP TABLE test_wide_not_nested;
