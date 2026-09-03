SET optimize_read_in_order = 1;
SET max_threads = 1;

CREATE TABLE t0 (c0 Nullable(Int64)) ENGINE = MergeTree() ORDER BY c0 SETTINGS allow_nullable_key=1;
INSERT INTO TABLE t0 VALUES (0);
INSERT INTO TABLE t0 VALUES (NULL), (1);

SELECT '--- table asc, query desc, last';
SELECT * FROM t0 ORDER BY c0 DESC NULLS LAST;
SELECT '--- table asc, query desc, first';
SELECT * FROM t0 ORDER BY c0 DESC NULLS FIRST;
SELECT '--- table asc, query asc, last';
SELECT * FROM t0 ORDER BY c0 ASC NULLS LAST;
SELECT '--- table asc, query asc, first';
SELECT * FROM t0 ORDER BY c0 ASC NULLS FIRST;

CREATE TABLE t1 (c0 Nullable(Int64)) ENGINE = MergeTree() ORDER BY c0 DESC SETTINGS allow_nullable_key=1, allow_experimental_reverse_key=1;
INSERT INTO TABLE t1 VALUES (0);
INSERT INTO TABLE t1 VALUES (NULL), (1);

SELECT '--- table desc, query desc, last';
SELECT * FROM t1 ORDER BY c0 DESC NULLS LAST;
SELECT '--- table desc, query desc, first';
SELECT * FROM t1 ORDER BY c0 DESC NULLS FIRST;
SELECT '--- table desc, query asc, last';
SELECT * FROM t1 ORDER BY c0 ASC NULLS LAST;
SELECT '--- table desc, query asc, first';
SELECT * FROM t1 ORDER BY c0 ASC NULLS FIRST;

CREATE TABLE f0 (c0 Float64) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO TABLE f0 VALUES (0);
INSERT INTO TABLE f0 VALUES (0/0), (1);

SELECT '--- table asc, query desc, last';
SELECT * FROM f0 ORDER BY c0 DESC NULLS LAST;
SELECT '--- table asc, query desc, first';
SELECT * FROM f0 ORDER BY c0 DESC NULLS FIRST;
SELECT '--- table asc, query asc, last';
SELECT * FROM f0 ORDER BY c0 ASC NULLS LAST;
SELECT '--- table asc, query asc, first';
SELECT * FROM f0 ORDER BY c0 ASC NULLS FIRST;

CREATE TABLE f1 (c0 Float64) ENGINE = MergeTree() ORDER BY c0 DESC SETTINGS allow_experimental_reverse_key=1;
INSERT INTO TABLE f1 VALUES (0);
INSERT INTO TABLE f1 VALUES (0/0), (1);

SELECT '--- table desc, query desc, last';
SELECT * FROM f1 ORDER BY c0 DESC NULLS LAST;
SELECT '--- table desc, query desc, first';
SELECT * FROM f1 ORDER BY c0 DESC NULLS FIRST;
SELECT '--- table desc, query asc, last';
SELECT * FROM f1 ORDER BY c0 ASC NULLS LAST;
SELECT '--- table desc, query asc, first';
SELECT * FROM f1 ORDER BY c0 ASC NULLS FIRST;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE lct0 (c0 LowCardinality(Nullable(Int64))) ENGINE = MergeTree() ORDER BY c0 SETTINGS allow_nullable_key=1;
INSERT INTO TABLE lct0 VALUES (0);
INSERT INTO TABLE lct0 VALUES (NULL), (1);
SELECT '--- table asc, query desc, last';
SELECT * FROM lct0 ORDER BY c0 DESC NULLS LAST;
SELECT '--- table asc, query desc, first';
SELECT * FROM lct0 ORDER BY c0 DESC NULLS FIRST;
SELECT '--- table asc, query asc, last';
SELECT * FROM lct0 ORDER BY c0 ASC NULLS LAST;
SELECT '--- table asc, query asc, first';
SELECT * FROM lct0 ORDER BY c0 ASC NULLS FIRST;
