DROP TABLE IF EXISTS datetime;
CREATE TABLE datetime (d DateTime('UTC')) ENGINE = Memory;
INSERT INTO datetime(d) VALUES(toDateTime('2016-06-15 23:00:00', 'UTC'))

SELECT quantile(0.2)(d), quantile(0.3)(d) FROM datetime;
SELECT quantileDeterministic(0.2)(d, 1), quantileDeterministic(0.5)(d, 1) FROM datetime;
SELECT quantileExact(0.2)(d), quantileExact(0.5)(d) FROM datetime;
SELECT quantileExactWeighted(0.2)(d, 1), quantileExactWeighted(0.4)(d, 1), quantileExactWeighted(0.3)(d, 2) FROM datetime;
SELECT quantileTiming(0.2)(d), quantileTiming(0.3)(d), quantileTiming(0.2)(d+1) FROM datetime;
SELECT quantileTimingWeighted(0.2)(d, 1), quantileTimingWeighted(0.3)(d, 1), quantileTimingWeighted(0.2)(d, 2) FROM datetime;
SELECT quantileTDigest(0.2)(d), quantileTDigest(0.3)(d), quantileTDigest(0.2)(d + 1) FROM datetime;
SELECT quantileTDigestWeighted(0.2)(d, 1), quantileTDigestWeighted(0.3)(d, 1), quantileTDigestWeighted(0.4)(d, 2) FROM datetime;
SELECT quantileBFloat16(0.2)(d), quantileBFloat16(0.3)(d), quantileBFloat16(0.4)(d + 1) FROM datetime;


SELECT '---------After fuse result-----------';
set optimize_fuse_quantile=true;
SELECT 'quantile:';
EXPLAIN SYNTAX SELECT quantile(0.2)(d), quantile(0.3)(d) FROM datetime;
SELECT quantile(0.2)(d), quantile(0.3)(d) FROM datetime;

SELECT 'quantileDeterministic:';
EXPLAIN SYNTAX SELECT quantileDeterministic(0.2)(d, 1), quantileDeterministic(0.5)(d, 1) FROM datetime;
SELECT quantileDeterministic(0.2)(d, 1), quantileDeterministic(0.5)(d, 1) FROM datetime;

SELECT 'quantileExact:';
EXPLAIN SYNTAX SELECT quantileExact(0.2)(d), quantileExact(0.5)(d) FROM datetime;
SELECT quantileExact(0.2)(d), quantileExact(0.5)(d) FROM datetime;

SELECT 'quantileExactWeighted:';
EXPLAIN SYNTAX SELECT quantileExactWeighted(0.2)(d, 1), quantileExactWeighted(0.4)(d, 1), quantileExactWeighted(0.3)(d, 2) FROM datetime;
SELECT quantileExactWeighted(0.2)(d, 1), quantileExactWeighted(0.4)(d, 1), quantileExactWeighted(0.3)(d, 2) FROM datetime;

SELECT 'quantileTiming:';
EXPLAIN SYNTAX SELECT quantileTiming(0.2)(d), quantileTiming(0.3)(d), quantileTiming(0.2)(d+1) FROM datetime;
SELECT quantileTiming(0.2)(d), quantileTiming(0.3)(d), quantileTiming(0.2)(d+1) FROM datetime;

SELECT 'quantileTimingWeighted:';
EXPLAIN SYNTAX SELECT quantileTimingWeighted(0.2)(d, 1), quantileTimingWeighted(0.3)(d, 1), quantileTimingWeighted(0.2)(d, 2) FROM datetime;
SELECT quantileTimingWeighted(0.2)(d, 1), quantileTimingWeighted(0.3)(d, 1), quantileTimingWeighted(0.2)(d, 2) FROM datetime;

SELECT 'quantileTDigest:';
EXPLAIN SYNTAX SELECT quantileTDigest(0.2)(d), quantileTDigest(0.3)(d), quantileTDigest(0.2)(d + 1) FROM datetime;
SELECT quantileTDigest(0.2)(d), quantileTDigest(0.3)(d), quantileTDigest(0.2)(d + 1) FROM datetime;

SELECT 'quantileTDigestWeighted:';
EXPLAIN SYNTAX SELECT quantileTDigestWeighted(0.2)(d, 1), quantileTDigestWeighted(0.3)(d, 1), quantileTDigestWeighted(0.4)(d, 2) FROM datetime;
SELECT quantileTDigestWeighted(0.2)(d, 1), quantileTDigestWeighted(0.3)(d, 1), quantileTDigestWeighted(0.4)(d, 2) FROM datetime;

SELECT 'quantileBFloat16:';
EXPLAIN SYNTAX SELECT quantileBFloat16(0.2)(d), quantileBFloat16(0.3)(d), quantileBFloat16(0.4)(d + 1) FROM datetime;
SELECT quantileBFloat16(0.2)(d), quantileBFloat16(0.3)(d), quantileBFloat16(0.4)(d + 1) FROM datetime;

EXPLAIN SYNTAX SELECT quantile(0.2)(d) as k, quantile(0.3)(d) FROM datetime order by quantile(0.2)(d);
DROP TABLE datetime;

SELECT b, quantile(0.5)(x) as a, quantile(0.9)(x) as y, quantile(0.95)(x) FROM (select number as x, number % 2 as b from numbers(10)) group by b;
EXPLAIN SYNTAX SELECT b, quantile(0.5)(x) as a, quantile(0.9)(x) as y, quantile(0.95)(x) FROM (select number as x, number % 2 as b from numbers(10)) group by b;
