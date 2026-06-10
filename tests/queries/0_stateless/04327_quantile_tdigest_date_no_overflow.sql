-- Tags: no-random-settings

-- Exact reporter dataset from issue #106722.
DROP TABLE IF EXISTS users_04327;
CREATE TABLE users_04327 (date Date, age UInt16) ENGINE = Memory;
INSERT INTO users_04327 VALUES ('2019-01-01', 33), ('1999-01-10', 48), ('2000-02-07', 50);

SELECT 'weighted Date';
SELECT quantileTDigestWeighted(date, age) FROM users_04327;

-- The plural variant always worked (no strict-equality check); singular must match it now.
SELECT 'weighted Date plural';
SELECT quantilesTDigestWeighted(0.5)(date, age) FROM users_04327;

-- Same class of bug for unweighted singular when interpolation yields a fractional day.
SELECT 'unweighted Date interpolated';
SELECT quantileTDigest(0.5)(d) FROM (SELECT toDate('1999-01-10') AS d UNION ALL SELECT toDate('2000-02-07'));

SELECT 'DateTime';
SELECT quantileTDigestWeighted(dt, w) FROM (SELECT toDateTime('2019-01-01 00:00:01') AS dt, toUInt64(33) AS w UNION ALL SELECT toDateTime('1999-01-10 00:00:00'), 48 UNION ALL SELECT toDateTime('2000-02-07 00:00:00'), 50);

-- Workarounds the reporter listed must keep working.
SELECT 'workarounds';
SELECT toDate(quantileTDigestWeighted(toUInt32(date), age)) FROM users_04327;
SELECT quantileExactWeighted(date, age) FROM users_04327;
SELECT quantileInterpolatedWeighted(date, age) FROM users_04327;

DROP TABLE users_04327;
