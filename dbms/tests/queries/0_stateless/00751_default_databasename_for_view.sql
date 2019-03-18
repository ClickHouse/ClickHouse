USE test;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_mv;
DROP TABLE IF EXISTS u;
DROP TABLE IF EXISTS v;

CREATE TABLE t
(
    date Date,
    platform Enum8('a' = 0, 'b' = 1),
    app Enum8('a' = 0, 'b' = 1)
) ENGINE = Memory;

CREATE TABLE u (app Enum8('a' = 0, 'b' = 1)) ENGINE = Memory;
CREATE TABLE v (platform Enum8('a' = 0, 'b' = 1)) ENGINE = Memory;

INSERT INTO u VALUES ('b');
INSERT INTO v VALUES ('b');

CREATE MATERIALIZED VIEW t_mv ENGINE = MergeTree ORDER BY date
    AS SELECT date, platform, app FROM t
    WHERE app = (SELECT min(app) from u) AND platform = (SELECT (SELECT min(platform) from v));

SHOW CREATE TABLE test.t_mv FORMAT TabSeparatedRaw;

USE default;
DETACH TABLE test.t_mv;
ATTACH TABLE test.t_mv;

INSERT INTO test.t VALUES ('2000-01-01', 'a', 'a') ('2000-01-02', 'b', 'b');

INSERT INTO test.u VALUES ('a');
INSERT INTO test.v VALUES ('a');

INSERT INTO test.t VALUES ('2000-01-03', 'a', 'a') ('2000-01-04', 'b', 'b');

SELECT * FROM test.t ORDER BY date;
SELECT * FROM test.t_mv ORDER BY date;

DROP TABLE test.t;
DROP TABLE test.t_mv;
DROP TABLE test.u;
DROP TABLE test.v;
