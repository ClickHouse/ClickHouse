SELECT * FROM (SELECT toUInt256(1)) AS t, (SELECT greatCircleAngle(toLowCardinality(toNullable(toUInt256(1048575))), 257, -9223372036854775808, 1048576), 1048575, materialize(2)) AS u;


SET join_algorithm='hash';
SET allow_experimental_join_condition=1;
SELECT * FROM ( SELECT 1 AS a, toLowCardinality(1), 1) AS t1 CROSS  JOIN (SELECT toLowCardinality(1 AS a), 1 AS b) AS t2;

