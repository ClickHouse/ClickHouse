-- Regression test: LEFT OUTER JOIN with join_use_nulls=1, join_algorithm='full_sorting_merge',
-- and JSON/Array(JSON) columns in the right table.
-- Crash: ColumnObject miscast as ColumnNullable during serialization for unmatched rows.

SET allow_experimental_json_type = 1;
SET max_threads = 16;
SET join_algorithm = 'full_sorting_merge';
SET join_use_nulls = 1;

CREATE TABLE t_fsm_left
(
    id String,
    name String
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE t_fsm_right_json
(
    id String,
    val JSON,
    arr Array(JSON)
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE t_fsm_right2_json
(
    `id` String,
    `result` Array(JSON),
    `tags` Array(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_fsm_left SELECT
    toString(number),
    toString(number)
FROM numbers(1000);

INSERT INTO t_fsm_right_json SELECT
    toString(number),
    '{"port":443}',
    ['{"proto":"tcp"}']
FROM numbers(500);

INSERT INTO t_fsm_right2_json SELECT
    toString(number),
    ['{"ok":true}'],
    ['tag1']
FROM numbers(300);

SELECT
    l.id,
    l.name,
    r1.val,
    r1.arr,
    r2.result,
    r2.tags
FROM t_fsm_left AS l
LEFT JOIN t_fsm_right_json AS r1 ON l.id = r1.id
LEFT JOIN t_fsm_right2_json AS r2 ON l.id = r2.id
ORDER BY l.id ASC
LIMIT 5;

SELECT
    l.id,
    l.name,
    r1.val,
    r1.arr,
    r2.result,
    r2.tags
FROM t_fsm_left AS l
LEFT OUTER JOIN t_fsm_right_json AS r1 ON l.id = r1.id
LEFT OUTER JOIN t_fsm_right2_json AS r2 ON l.id = r2.id
ORDER BY l.id
SETTINGS join_use_nulls = 1, join_algorithm = 'full_sorting_merge';

DROP TABLE t_fsm_left;
DROP TABLE t_fsm_right_json;
DROP TABLE t_fsm_right2_json;
