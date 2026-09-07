set allow_reorder_prewhere_conditions=0;

drop table if exists t1;
drop table if exists t2;

CREATE TABLE t1
(
    `s1` String,
    `s2` String,
    `s3` String
)
ENGINE = MergeTree
ORDER BY tuple();


CREATE TABLE t2
(
    `fs1` FixedString(10),
    `fs2` FixedString(10)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t1 SELECT
    repeat('t', 15) s1,
    'test' s2,
    'test' s3;

INSERT INTO t1 SELECT
    substring(s1, 1, 10),
    s2,
    s3
FROM generateRandom('s1 String, s2 String, s3 String')
LIMIT 10000;

INSERT INTO t2 SELECT *
FROM generateRandom()
LIMIT 10000;

WITH
tmp1 AS
(
    SELECT
        CAST(s1, 'FixedString(10)') AS fs1,
        s2 AS sector,
        s3
    FROM t1
    WHERE  (s3 != 'test')
)
    SELECT
        fs1
    FROM t2
    LEFT JOIN tmp1 USING (fs1)
    WHERE (fs1 IN ('test')) SETTINGS enable_multiple_prewhere_read_steps = 0, query_plan_merge_filters=0;

WITH
tmp1 AS
(
    SELECT
        CAST(s1, 'FixedString(10)') AS fs1,
        s2 AS sector,
        s3
    FROM t1
    WHERE  (s3 != 'test')
)
    SELECT
        fs1
    FROM t2
    LEFT JOIN tmp1 USING (fs1)
    WHERE (fs1 IN ('test')) SETTINGS enable_multiple_prewhere_read_steps = 1, query_plan_merge_filters=1;

optimize table t1 final;

WITH
tmp1 AS
(
    SELECT
        CAST(s1, 'FixedString(10)') AS fs1,
        s2 AS sector,
        s3
    FROM t1
    WHERE  (s3 != 'test')
)
    SELECT
        fs1
    FROM t2
    LEFT JOIN tmp1 USING (fs1)
    WHERE (fs1 IN ('test')) SETTINGS enable_multiple_prewhere_read_steps = 0, query_plan_merge_filters=0;

WITH
tmp1 AS
(
    SELECT
        CAST(s1, 'FixedString(10)') AS fs1,
        s2 AS sector,
        s3
    FROM t1
    WHERE  (s3 != 'test')
)
    SELECT
        fs1
    FROM t2
    LEFT JOIN tmp1 USING (fs1)
    WHERE (fs1 IN ('test')) SETTINGS enable_multiple_prewhere_read_steps = 1, query_plan_merge_filters=1;
