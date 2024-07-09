SET allow_deprecated_error_prone_window_functions = 1;
drop table if exists largestTriangleThreeBucketsTestFloat64Float64;

CREATE TABLE largestTriangleThreeBucketsTestFloat64Float64
(
    x Float64,
    y Float64
) ENGINE = MergeTree order by (y,x);

INSERT INTO largestTriangleThreeBucketsTestFloat64Float64
VALUES (1.0, 10.0),(2.0, 20.0),(3.0, 15.0),(8.0, 60.0),(9.0, 55.0),(10.0, 70.0),(4.0, 30.0),(5.0, 40.0),(6.0, 35.0),(7.0, 50.0);

select largestTriangleThreeBuckets(0)(x, y) FROM largestTriangleThreeBucketsTestFloat64Float64;

select largestTriangleThreeBuckets(1)(x, y) FROM largestTriangleThreeBucketsTestFloat64Float64;

select largestTriangleThreeBuckets(2)(x, y) FROM largestTriangleThreeBucketsTestFloat64Float64;

SELECT largestTriangleThreeBuckets(4)(x, y) AS downsampled_data
FROM largestTriangleThreeBucketsTestFloat64Float64;

drop table largestTriangleThreeBucketsTestFloat64Float64;

drop table if exists largestTriangleThreeBucketsTestDecimal64Decimal64;

CREATE TABLE largestTriangleThreeBucketsTestDecimal64Decimal64
(
    x Decimal64(2),
    y Decimal64(2)
) ENGINE = MergeTree order by (y,x);

INSERT INTO largestTriangleThreeBucketsTestDecimal64Decimal64(x, y) VALUES (0.63, 0.25), (0.02, 0.16), (0.29, 0.16), (0.2, 0.24), (0.41, 0.63), (0.06, 0.73), (0.36, 0.99), (0.57, 0.18), (0.98, 0.09), (0.73, 0.95), (0.45, 0.86), (0.37, 0.86), (0.6, 0.64), (0.11, 0.31), (0.7, 0.25), (0.85, 0.15), (0.68, 0.39), (0.9, 0.3), (0.25, 0.34), (0.09, 0.0), (0.91, 0.62), (0.47, 0.06), (0.08, 0.88), (0.48, 0.57), (0.55, 0.75), (0.19, 0.27), (0.87, 0.15), (0.15, 0.09), (0.77, 0.28), (0.5, 0.2), (0.39, 0.86), (0.52, 0.11), (0.38, 0.75), (0.71, 0.44), (0.21, 0.46), (0.88, 0.15), (0.83, 0.67), (0.23, 0.23);

select largestTriangleThreeBuckets(20)(x, y) from largestTriangleThreeBucketsTestDecimal64Decimal64;

drop table largestTriangleThreeBucketsTestDecimal64Decimal64;

drop table if exists largestTriangleThreeBucketsTestDateTime64Float64;

create table largestTriangleThreeBucketsTestDateTime64Float64 (x DateTime64(3), y Float64) engine = MergeTree order by (y,x);

INSERT INTO largestTriangleThreeBucketsTestDateTime64Float64 (x, y) VALUES ('2023-09-06 00:00:00', 14.217481939467213), ('2023-09-11 00:00:00', 30.096113766096455), ('2023-01-31 00:00:00', 91.42364224984735), ('2023-12-14 00:00:00', 42.08543753438961), ('2023-10-31 00:00:00', 29.93227107709394), ('2023-12-31 00:00:00', 98.52375935588333), ('2023-07-07 00:00:00', 79.9367415060134), ('2023-08-02 00:00:00', 55.417182033825696), ('2023-03-15 00:00:00', 98.77709508458238), ('2023-09-05 00:00:00', 2.832505232031368), ('2023-06-05 00:00:00', 8.107958052612418), ('2023-02-08 00:00:00', 62.95788480328096), ('2023-02-17 00:00:00', 76.80522155552535), ('2023-11-13 00:00:00', 24.927527306242993), ('2023-02-03 00:00:00', 7.966981342350332), ('2023-05-31 00:00:00', 44.61922229800436), ('2023-09-21 00:00:00', 65.86974701469791), ('2023-01-14 00:00:00', 35.96528042030847), ('2023-02-19 00:00:00', 16.065599678978305), ('2023-05-24 00:00:00', 17.23630978966909), ('2023-11-15 00:00:00', 15.544172190379879), ('2023-12-03 00:00:00', 13.738382187690856), ('2023-10-09 00:00:00', 16.7137129521176), ('2023-11-19 00:00:00', 12.12866001303361), ('2023-06-10 00:00:00', 95.15764263905534), ('2023-07-06 00:00:00', 18.87765798627088), ('2023-03-13 00:00:00', 44.82941460384813), ('2023-01-29 00:00:00', 36.0214717111606), ('2023-12-19 00:00:00', 90.30173319497655), ('2023-07-15 00:00:00', 12.67101467231364), ('2023-07-06 00:00:00', 88.13662733228512), ('2023-05-10 00:00:00', 34.18711141027026), ('2023-11-12 00:00:00', 75.58716684321973), ('2023-10-28 00:00:00', 35.79179186729331), ('2023-11-14 00:00:00', 0.9318182359137728), ('2023-09-29 00:00:00', 80.05338096818797), ('2023-09-13 00:00:00', 16.130217942056866), ('2023-07-28 00:00:00', 11.186638594914744), ('2023-02-12 00:00:00', 69.43690757793445), ('2023-12-18 00:00:00', 12.832032764204616), ('2023-05-21 00:00:00', 74.25002458036471), ('2023-04-03 00:00:00', 51.5662427420719), ('2023-11-27 00:00:00', 96.44359131281784), ('2023-03-29 00:00:00', 33.018594418113324), ('2023-02-07 00:00:00', 84.58945099939815), ('2023-11-16 00:00:00', 40.61531555527268), ('2023-04-21 00:00:00', 60.0545791577218), ('2023-01-31 00:00:00', 87.23185155362057), ('2023-05-19 00:00:00', 77.4095289464808), ('2023-08-26 00:00:00', 18.700816570182067);

select largestTriangleThreeBuckets(5)(x, y) from largestTriangleThreeBucketsTestDateTime64Float64;

select lttb(5)(x, y) from largestTriangleThreeBucketsTestDateTime64Float64;

drop table largestTriangleThreeBucketsTestDateTime64Float64;

CREATE TABLE largestTriangleTreeBucketsBucketSizeTest
(
    x UInt32,
    y UInt32
) ENGINE = MergeTree ORDER BY x;

INSERT INTO largestTriangleTreeBucketsBucketSizeTest (x, y) SELECT (number + 1) AS x, (x % 1000) AS y FROM numbers(9999);

SELECT
  arrayJoin(lttb(1000)(x, y)) AS point,
  tupleElement(point, 1) AS point_x,
  point_x - neighbor(point_x, -1) AS point_x_diff_with_previous_row
FROM largestTriangleTreeBucketsBucketSizeTest LIMIT 990, 10;

DROP TABLE largestTriangleTreeBucketsBucketSizeTest;
