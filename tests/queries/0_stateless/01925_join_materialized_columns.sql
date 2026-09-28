DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (
    time DateTime,
    foo String,
    dimension_1 String,
    dt Date MATERIALIZED toDate(time),
    dt1 Date MATERIALIZED toDayOfYear(time),
    aliascol1 ALIAS foo || dimension_1,
    time_alias DateTime ALIAS time
) ENGINE = MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY (dt, foo);

CREATE TABLE t2 (
    time DateTime,
    bar String,
    dimension_2 String,
    dt Date MATERIALIZED toDate(time),
    dt2 Date MATERIALIZED toDayOfYear(time),
    aliascol2 ALIAS bar || dimension_2,
    time_alias DateTime ALIAS time
) ENGINE = MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY (dt, bar);

INSERT INTO t1 VALUES ('2020-01-01 12:00:00', 'fact1', 't1_val1'), ('2020-02-02 13:00:00', 'fact2', 't1_val2'), ('2020-01-01 13:00:00', 'fact3', 't1_val3');
INSERT INTO t2 VALUES ('2020-01-01 12:00:00', 'fact1', 't2_val2'), ('2020-02-05 13:00:00', 'fact2', 't1_val2'), ('2019-01-01 12:00:00', 'fact4', 't2_val2');

SELECT * FROM t1 JOIN t2 ON t1.foo = t2.bar WHERE t2.dt >= '2020-02-01';
SELECT '-';
SELECT t1.*, t1.dt, t2.*, t2.dt FROM t1 JOIN t2 ON t1.foo = t2.bar WHERE t2.dt >= '2020-02-01';
SELECT '-';
SELECT t1.dt, t2.dt FROM t1 JOIN t2 ON t1.foo = t2.bar ORDER BY t1.dt;
SELECT '-';
SELECT * FROM t1 ALL JOIN t2 ON t1.dt = t2.dt ORDER BY t1.time, t2.time;
SELECT '-';
SELECT * FROM t1 ALL JOIN t2 USING (dt) ORDER BY t1.time, t2.time settings enable_analyzer=0;
SELECT * FROM t1 ALL JOIN t2 USING (dt) ORDER BY t1.time, t2.time settings enable_analyzer=1;
SELECT '-';
SELECT * FROM t1 JOIN t2 ON t1.dt1 = t2.dt2 ORDER BY t1.time, t1.dimension_1, t2.time, t2.dimension_2;
SELECT '-';
SELECT * FROM t1 JOIN t2 ON t1.foo = t2.bar WHERE t2.aliascol2 == 'fact2t1_val2';
SELECT '-';
SELECT t1.aliascol1, t2.aliascol2 FROM t1 JOIN t2 ON t1.foo = t2.bar ORDER BY t1.time, t2.time;
SELECT '-';
SELECT t1.time, t2.time FROM t1 JOIN t2 ON t1.aliascol1 = t2.aliascol2 ORDER BY t1.time, t2.time;
SELECT '-';
SELECT count() FROM t1 JOIN t2 ON t1.time_alias = t2.time;
SELECT count() FROM t1 JOIN t2 ON t1.time = t2.time_alias;
SELECT count() FROM t1 JOIN t2 ON t1.time_alias = t2.time_alias;
SELECT count() FROM t1 JOIN t2 USING (time_alias);
SELECT '-';
SELECT t1.time as talias FROM t1 JOIN t2 ON talias = t2.time;
SELECT t1.time as talias FROM t1 JOIN t2 ON talias = t2.time_alias;
SELECT t2.time as talias FROM t1 JOIN t2 ON t1.time = talias;
SELECT t2.time as talias FROM t1 JOIN t2 ON t1.time_alias = talias;
SELECT time as talias FROM t1 JOIN t2 ON talias = t2.time settings enable_analyzer=0; -- { serverError AMBIGUOUS_COLUMN_NAME }
SELECT time as talias FROM t1 JOIN t2 ON talias = t2.time settings enable_analyzer=1;

SELECT time as talias FROM t1 JOIN t2 ON t1.time = talias ORDER BY ALL settings enable_analyzer=1;
SELECT * FROM t1 JOIN t2 ON t1.time = t2.time AND NULL ORDER BY ALL settings enable_analyzer=1;
