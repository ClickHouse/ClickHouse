-- 00089_group_by_arrays_of_fixed.sql
CREATE TABLE numbers_10000 ENGINE = Memory()
    AS SELECT number FROM system.numbers LIMIT 10000;

CREATE TABLE test1_00089 ENGINE = AggregatingMemory() 
    AS SELECT arrayMap(x -> x % 2, groupArray(number)) AS arr FROM numbers_10000 GROUP BY number % ((number * 0xABCDEF0123456789 % 1234) + 1);
INSERT INTO test1_00089 SELECT * FROM numbers_10000;

CREATE TABLE test2_00089 ENGINE = AggregatingMemory()
    AS SELECT arr, count() AS c FROM test1_00089 GROUP BY arr;
INSERT INTO test2_00089 SELECT * FROM test1_00089;

SELECT * FROM test2_00089 ORDER BY c DESC, arr ASC;

-- 01786_group_by_pk_many_streams.sql
CREATE TABLE group_by_pk (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY k PARTITION BY v % 50;
INSERT INTO group_by_pk SELECT number / 100, number FROM numbers(1000);

CREATE TABLE test1_01786 ENGINE = AggregatingMemory() 
    AS SELECT sum(v) AS s FROM group_by_pk GROUP BY k;
INSERT INTO test1_01786 SELECT * FROM group_by_pk;

SELECT * FROM test1_01786 ORDER BY s DESC LIMIT 5;

-- 00041_aggregation_remap.sql
CREATE TABLE numbers_200000 ENGINE = Memory()
    AS SELECT number FROM system.numbers LIMIT 200000;

CREATE TABLE test1_00041 ENGINE = AggregatingMemory() 
    AS SELECT number, count() FROM numbers_200000 GROUP BY number;
INSERT INTO test1_00041 SELECT * FROM numbers_200000;

SELECT number, `count()` FROM test1_00041 ORDER BY `count()`, number LIMIT 10;

-- 00134_aggregation_by_fixed_string_of_size_1_2_4_8.sql
CREATE TABLE test1_00134 ENGINE = AggregatingMemory() 
    AS SELECT materialize(toFixedString('', 1)) AS x FROM system.one GROUP BY x;
INSERT INTO test1_00134 SELECT * FROM system.one;

SELECT * FROM test1_00134;

-- 00181_aggregate_functions_statistics.sql
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);

CREATE TABLE x_values(x_value Float64) ENGINE = Memory;
CREATE TABLE xy_values(x_value Float64, y_value Float64) ENGINE = Memory;

/* varSamp */
CREATE TABLE test1_00181 ENGINE = AggregatingMemory() 
    AS SELECT varSamp(x_value) FROM x_values;

INSERT INTO test1_00181 SELECT x_value FROM series LIMIT 0;
SELECT * FROM test1_00181;

INSERT INTO test1_00181 SELECT x_value FROM series LIMIT 1;
SELECT * FROM test1_00181;


CREATE TABLE test2_00181 ENGINE = AggregatingMemory() 
AS SELECT
    varSamp(x_value) AS res1,
    (sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / (count() - 1) AS res2
FROM series;
INSERT INTO test2_00181 SELECT * FROM series;

SELECT round(abs(res1 - res2), 6) FROM test2_00181;
SELECT round(abs(res1 - res2), 6) FROM test2_00181;

/* stddevSamp */
CREATE TABLE test3_00181 ENGINE = AggregatingMemory() 
    AS SELECT stddevSamp(x_value) FROM x_values;

INSERT INTO test3_00181 SELECT x_value FROM series LIMIT 0;
SELECT * FROM test3_00181;

INSERT INTO test3_00181 SELECT x_value FROM series LIMIT 1;
SELECT * FROM test3_00181;


CREATE TABLE test4_00181 ENGINE = AggregatingMemory() 
AS SELECT
    stddevSamp(x_value) AS res1,
    sqrt((sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / (count() - 1)) AS res2
FROM series;
INSERT INTO test4_00181 SELECT * FROM series;

SELECT round(abs(res1 - res2), 6) FROM test4_00181;
SELECT round(abs(res1 - res2), 6) FROM test4_00181;

/* skewSamp */
CREATE TABLE test5_00181 ENGINE = AggregatingMemory() 
    AS SELECT skewSamp(x_value) FROM x_values;

INSERT INTO test5_00181 SELECT x_value FROM series LIMIT 0;
SELECT * FROM test5_00181;

INSERT INTO test5_00181 SELECT x_value FROM series LIMIT 1;
SELECT * FROM test5_00181;


CREATE TABLE test6_00181 ENGINE = AggregatingMemory() 
AS SELECT
    skewSamp(x_value) AS res1,
    (
        sum(x_value * x_value * x_value) / count()
        - 3 * sum(x_value * x_value) / count() * sum(x_value) / count()
        + 2 * sum(x_value) / count() * sum(x_value) / count() * sum(x_value) / count()
    ) / pow((sum(x_value * x_value) - ((sum(x_value) * sum(x_value)) / count())) / (count() - 1), 1.5) AS res2
FROM series;
INSERT INTO test6_00181 SELECT * FROM series;

SELECT round(abs(res1 - res2), 6) FROM test6_00181;
SELECT round(abs(res1 - res2), 6) FROM test6_00181;


/* covarSamp */
CREATE TABLE test7_00181 ENGINE = AggregatingMemory() 
    AS SELECT covarSamp(x_value, y_value) FROM xy_values;

INSERT INTO test7_00181 SELECT x_value, y_value FROM series LIMIT 0;
SELECT * FROM test7_00181;

INSERT INTO test7_00181 SELECT x_value, y_value FROM series LIMIT 1;
SELECT * FROM test7_00181;

CREATE TABLE test8_00181 ENGINE = AggregatingMemory() 
AS SELECT
    toUInt32(arrayJoin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])) AS ID,
    avg(x_value) AS AVG_X,
    avg(y_value) AS AVG_Y
FROM series;
INSERT INTO test8_00181 SELECT * FROM series;

CREATE TABLE test9_00181 ENGINE = AggregatingMemory() 
AS SELECT
    arrayJoin([1]) AS ID2,
    covarSamp(x_value, y_value) AS COVAR1
FROM series;
INSERT INTO test9_00181 SELECT * FROM series;

SET joined_subquery_requires_alias = 0;

CREATE TABLE test10_00181 ENGINE = Memory()
AS SELECT
    (X - AVG_X) * (Y - AVG_Y) AS VAL
    FROM test8_00181 ANY INNER JOIN
    (
        SELECT
            i AS ID,
            x_value AS X,
            y_value AS Y
        FROM series
    ) USING ID;

CREATE TABLE test11_00181 ENGINE = AggregatingMemory() 
AS SELECT
    arrayJoin([1]) AS ID2,
    sum(VAL) / (count() - 1) AS COVAR2
FROM test10_00181;
INSERT INTO test11_00181 SELECT * FROM test10_00181;

SELECT round(abs(COVAR1 - COVAR2), 6)
FROM test9_00181 ANY INNER JOIN test11_00181 USING ID2;

SELECT round(abs(COVAR1 - COVAR2), 6)
FROM test9_00181 ANY INNER JOIN test11_00181 USING ID2;

/* corr */
CREATE TABLE test12_00181 ENGINE = AggregatingMemory() 
    AS SELECT corr(x_value, y_value) FROM xy_values;

INSERT INTO test12_00181 SELECT x_value, y_value FROM series LIMIT 0;
SELECT * FROM test12_00181;

INSERT INTO test12_00181 SELECT x_value, y_value FROM series LIMIT 1;
SELECT * FROM test12_00181;

CREATE TABLE test13_00181 ENGINE = AggregatingMemory() 
    AS SELECT round(abs(corr(x_value, y_value) - covarPop(x_value, y_value) / (stddevPop(x_value) * stddevPop(y_value))), 6) FROM series;
INSERT INTO test13_00181 SELECT * FROM series;

SELECT * FROM test13_00181;

/* quantile AND quantileExact */
SELECT '----quantile----';

CREATE TABLE numbers_90 ENGINE = Memory()
    AS SELECT * FROM numbers(90);

CREATE TABLE test14_00181 ENGINE = AggregatingMemory() 
    AS SELECT quantileExactIf(number, number > 0) FROM numbers_90;
INSERT INTO test14_00181 SELECT * FROM numbers_90;
SELECT * FROM test14_00181;


CREATE TABLE test15_00181 ENGINE = AggregatingMemory() 
    AS SELECT quantileExactIf(number, number > 100) FROM numbers_90;
INSERT INTO test15_00181 SELECT * FROM numbers_90;
SELECT * FROM test15_00181;

CREATE TABLE test16_00181 ENGINE = AggregatingMemory() 
    AS SELECT quantileExactIf(toFloat32(number) , number > 100) FROM numbers_90;
INSERT INTO test16_00181 SELECT * FROM numbers_90;
SELECT * FROM test16_00181;

CREATE TABLE test17_00181 ENGINE = AggregatingMemory() 
    AS SELECT quantileExactIf(toFloat64(number) , number > 100) FROM numbers_90;
INSERT INTO test17_00181 SELECT * FROM numbers_90;
SELECT * FROM test17_00181;


CREATE TABLE test18_00181 ENGINE = AggregatingMemory() 
    AS SELECT quantileIf(number, number > 100) FROM numbers_90;
INSERT INTO test18_00181 SELECT * FROM numbers_90;
SELECT * FROM test18_00181;

CREATE TABLE test19_00181 ENGINE = AggregatingMemory() 
    AS SELECT quantileIf(toFloat32(number) , number > 100) FROM numbers_90;
INSERT INTO test19_00181 SELECT * FROM numbers_90;
SELECT * FROM test19_00181;

CREATE TABLE test20_00181 ENGINE = AggregatingMemory() 
    AS SELECT quantileIf(toFloat64(number) , number > 100) FROM numbers_90;
INSERT INTO test20_00181 SELECT * FROM numbers_90;
SELECT * FROM test20_00181;
