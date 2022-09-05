SET max_merged_block_bytes_in_join=1024000;
DROP TABLE IF EXISTS join_opt_t1;
DROP TABLE IF EXISTS join_opt_t2;
DROP TABLE IF EXISTS join_opt_t3;

CREATE TABLE join_opt_t1(a int, b int)engine=MergeTree ORDER BY a;
CREATE TABLE join_opt_t2(a int, b int)engine=MergeTree ORDER BY a;
CREATE TABLE join_opt_t3(a int, b int)engine=MergeTree ORDER BY a;
INSERT INTO join_opt_t1 SELECT number, number+1 from numbers(1000);
INSERT INTO join_opt_t2 SELECT number, number+1 from numbers(200);
INSERT INTO join_opt_t3 SELECT number, number+1 from numbers(100);

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a
    INNER JOIN join_opt_t3
    ON join_opt_t2.b = join_opt_t3.b;

SELECT count(*)
    FROM join_opt_t1
    LEFT JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a
    LEFT JOIN join_opt_t3
    ON join_opt_t2.b = join_opt_t3.b;

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a AND join_opt_t1.a < 3;

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a AND join_opt_t1.b = join_opt_t2.b;

SELECT count(*)
    FROM join_opt_t1
    INNER JOIN join_opt_t2
    ON join_opt_t1.a = join_opt_t2.a OR join_opt_t1.b = join_opt_t2.b;

SELECT t1.a, t1.b
    FROM join_opt_t1 as t1
    INNER JOIN join_opt_t2 as t2
    ON t1.a = t2.a AND t1.a < 3;

DROP TABLE join_opt_t1;
DROP TABLE join_opt_t2;
DROP TABLE join_opt_t3;
