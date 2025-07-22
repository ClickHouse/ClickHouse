CREATE TABLE tableNull (k1 Nullable(Int32)) ENGINE = Memory;
CREATE TABLE joinInt (k1 Int32) ENGINE = Join(ALL, FULL, k1);

CREATE TABLE tableInt (k1 Int32) ENGINE = Memory;
CREATE TABLE joinNull (k1 Nullable(Int32)) ENGINE = Join(ALL, FULL, k1);

INSERT INTO tableNull VALUES (1), (3);
INSERT INTO joinInt VALUES (1), (2);

INSERT INTO tableInt VALUES (1), (3);
INSERT INTO joinNull VALUES (1), (2);

SET enable_analyzer=1;

-- { echoOn }
SELECT tableNull.k1, joinInt.k1 FROM tableNull FULL JOIN joinInt USING (k1) ORDER BY ALL;
SELECT tableNull.k1, t1.k1, t2.k1 FROM tableNull FULL JOIN joinInt t2 USING (k1) FULL JOIN joinInt t1 USING (k1) ORDER BY ALL;

SELECT k+1, t FROM (
    SELECT joinInt.k1 as k, toTypeName(joinInt.k1) as t FROM tableNull FULL JOIN joinInt USING (k1)
) ORDER BY ALL;

SELECT tableInt.k1, joinNull.k1 FROM tableInt FULL JOIN joinNull USING (k1) ORDER BY ALL;
SELECT tableInt.k1, t1.k1, t2.k1 FROM tableInt FULL JOIN joinNull t2 USING (k1) FULL JOIN joinNull t1 USING (k1) ORDER BY ALL;

SELECT k+1, t FROM (
    SELECT joinNull.k1 as k, toTypeName(joinNull.k1) as t FROM tableInt FULL JOIN joinNull USING (k1)
) ORDER BY ALL;

-- { echoOff }

DROP TABLE joinInt;
DROP TABLE joinNull;

CREATE TABLE joinInt (k1 Int32) ENGINE = Join(ALL, FULL, k1) SETTINGS join_use_nulls=1;
CREATE TABLE joinNull (k1 Nullable(Int32)) ENGINE = Join(ALL, FULL, k1) SETTINGS join_use_nulls=1;

INSERT INTO joinInt VALUES (1), (2);
INSERT INTO joinNull VALUES (1), (2);

SET join_use_nulls=1;

-- { echoOn }
SELECT tableNull.k1, joinInt.k1 FROM tableNull FULL JOIN joinInt USING (k1) ORDER BY ALL;
SELECT tableNull.k1, t1.k1, t2.k1 FROM tableNull FULL JOIN joinInt t2 USING (k1) FULL JOIN joinInt t1 USING (k1) ORDER BY ALL;

SELECT k+1, t FROM (
    SELECT joinInt.k1 as k, toTypeName(joinInt.k1) as t FROM tableNull FULL JOIN joinInt USING (k1)
) ORDER BY ALL;

SELECT tableInt.k1, joinNull.k1 FROM tableInt FULL JOIN joinNull USING (k1) ORDER BY ALL;
SELECT tableInt.k1, t1.k1, t2.k1 FROM tableInt FULL JOIN joinNull t2 USING (k1) FULL JOIN joinNull t1 USING (k1) ORDER BY ALL;

SELECT k+1, t FROM (
    SELECT joinNull.k1 as k, toTypeName(joinNull.k1) as t FROM tableInt FULL JOIN joinNull USING (k1)
) ORDER BY ALL;
