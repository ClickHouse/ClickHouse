DROP TABLE IF EXISTS test.datetime;

CREATE TABLE test.datetime (d DateTime) ENGINE = Memory;
INSERT INTO test.datetime(d) VALUES(toDateTime('2016-06-15 23:00:00'));

SELECT quantile(0.2)(d) FROM test.datetime;
SELECT quantiles(0.2)(d) FROM test.datetime;

SELECT quantileDeterministic(0.2)(d, 1) FROM test.datetime;
SELECT quantilesDeterministic(0.2)(d, 1) FROM test.datetime;

SELECT quantileExact(0.2)(d) FROM test.datetime;
SELECT quantilesExact(0.2)(d) FROM test.datetime;

SELECT quantileExactWeighted(0.2)(d, 1) FROM test.datetime;
SELECT quantilesExactWeighted(0.2)(d, 1) FROM test.datetime;

SELECT quantileTiming(0.2)(d) FROM test.datetime;
SELECT quantilesTiming(0.2)(d) FROM test.datetime;

SELECT quantileTimingWeighted(0.2)(d, 1) FROM test.datetime;
SELECT quantilesTimingWeighted(0.2)(d, 1) FROM test.datetime;

SELECT quantileTDigest(0.2)(d) FROM test.datetime;
SELECT quantilesTDigest(0.2)(d) FROM test.datetime;

SELECT quantileTDigestWeighted(0.2)(d, 1) FROM test.datetime;
SELECT quantilesTDigestWeighted(0.2)(d, 1) FROM test.datetime;

DROP TABLE test.datetime;
