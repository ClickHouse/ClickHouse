DROP TABLE IF EXISTS datetime;

CREATE TABLE datetime (d DateTime('UTC')) ENGINE = Memory;
INSERT INTO datetime(d) VALUES(toDateTime('2016-06-15 23:00:00', 'UTC'));

SELECT quantile(0.2)(d) FROM datetime;
SELECT quantiles(0.2)(d) FROM datetime;

SELECT quantileDeterministic(0.2)(d, 1) FROM datetime;
SELECT quantilesDeterministic(0.2)(d, 1) FROM datetime;

SELECT quantileExact(0.2)(d) FROM datetime;
SELECT quantilesExact(0.2)(d) FROM datetime;

SELECT quantileExactWeighted(0.2)(d, 1) FROM datetime;
SELECT quantilesExactWeighted(0.2)(d, 1) FROM datetime;

SELECT quantileInterpolatedWeighted(0.2)(d, 1) FROM datetime;
SELECT quantilesInterpolatedWeighted(0.2)(d, 1) FROM datetime;

SELECT quantileExactWeightedInterpolated(0.2)(d, 1) FROM datetime;
SELECT quantilesExactWeightedInterpolated(0.2)(d, 1) FROM datetime;

SELECT quantileTiming(0.2)(d) FROM datetime;
SELECT quantilesTiming(0.2)(d) FROM datetime;

SELECT quantileTimingWeighted(0.2)(d, 1) FROM datetime;
SELECT quantilesTimingWeighted(0.2)(d, 1) FROM datetime;

SELECT quantileTDigest(0.2)(d) FROM datetime;
SELECT quantilesTDigest(0.2)(d) FROM datetime;

SELECT quantileTDigestWeighted(0.2)(d, 1) FROM datetime;
SELECT quantilesTDigestWeighted(0.2)(d, 1) FROM datetime;

SELECT quantileBFloat16(0.2)(d) FROM datetime;
SELECT quantilesBFloat16(0.2)(d) FROM datetime;

SELECT quantileBFloat16Weighted(0.2)(d, 1) FROM datetime;
SELECT quantilesBFloat16Weighted(0.2)(d, 1) FROM datetime;

DROP TABLE datetime;
