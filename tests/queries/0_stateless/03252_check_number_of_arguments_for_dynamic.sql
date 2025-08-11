set enable_analyzer=1;
set enable_json_type=1;

CREATE TABLE t
(
    `a` JSON
)
ENGINE = MergeTree()
ORDER BY tuple();

insert into t values ('{"a":1}'), ('{"a":2.0}');

SELECT 1
FROM
(
    SELECT 1 AS c0
) AS tx
FULL OUTER JOIN t AS t2 ON equals(t2.a.Float32); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
