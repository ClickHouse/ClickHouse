USE test;

DROP TABLE IF EXISTS t_00751;
DROP TABLE IF EXISTS t_mv_00751;
DROP TABLE IF EXISTS u_00751;
DROP TABLE IF EXISTS v_00751;

CREATE TABLE t_00751
(
    date Date,
    platform Enum8('a' = 0, 'b' = 1),
    app Enum8('a' = 0, 'b' = 1)
) ENGINE = Memory;

CREATE TABLE u_00751 (app Enum8('a' = 0, 'b' = 1)) ENGINE = Memory;
CREATE TABLE v_00751 (platform Enum8('a' = 0, 'b' = 1)) ENGINE = Memory;

INSERT INTO u_00751 VALUES ('b');
INSERT INTO v_00751 VALUES ('b');

CREATE MATERIALIZED VIEW t_mv_00751 ENGINE = MergeTree ORDER BY date
    AS SELECT date, platform, app FROM t_00751
    WHERE app = (SELECT min(app) from u_00751) AND platform = (SELECT (SELECT min(platform) from v_00751));

SHOW CREATE TABLE test.t_mv_00751 FORMAT TabSeparatedRaw;

USE default;
DETACH TABLE test.t_mv_00751;
ATTACH TABLE test.t_mv_00751;

INSERT INTO test.t_00751 VALUES ('2000-01-01', 'a', 'a') ('2000-01-02', 'b', 'b');

INSERT INTO test.u_00751 VALUES ('a');
INSERT INTO test.v_00751 VALUES ('a');

INSERT INTO test.t_00751 VALUES ('2000-01-03', 'a', 'a') ('2000-01-04', 'b', 'b');

SELECT * FROM test.t_00751 ORDER BY date;
SELECT * FROM test.t_mv_00751 ORDER BY date;

DROP TABLE test.t_00751;
DROP TABLE test.t_mv_00751;
DROP TABLE test.u_00751;
DROP TABLE test.v_00751;
