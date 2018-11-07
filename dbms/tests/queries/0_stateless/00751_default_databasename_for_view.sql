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
    WHERE app = (SELECT min(app) from u) AND platform = (SELECT min(platform) from v);

SHOW CREATE TABLE test.t_mv FORMAT TabSeparatedRaw;

INSERT INTO t VALUES ('2000-01-01', 'a', 'a') ('2000-01-02', 'b', 'b');

INSERT INTO u VALUES ('a');
INSERT INTO v VALUES ('a');

INSERT INTO t VALUES ('2000-01-03', 'a', 'a') ('2000-01-04', 'b', 'b');

SELECT * FROM t ORDER BY date;
SELECT * FROM t_mv ORDER BY date;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_mv;
