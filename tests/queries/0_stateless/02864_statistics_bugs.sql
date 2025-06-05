SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET mutations_sync = 1;

DROP TABLE IF EXISTS bug_67742;
CREATE TABLE bug_67742 (a Float64 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple();
INSERT INTO bug_67742 SELECT number FROM system.numbers LIMIT 10000;
SELECT count(*) FROM bug_67742 WHERE a < '10';
DROP TABLE bug_67742;

DROP TABLE IF EXISTS bug_67742;
CREATE TABLE bug_67742 (a Int32 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple();
INSERT INTO bug_67742 SELECT number FROM system.numbers LIMIT 10000;
SELECT count(*) FROM bug_67742 WHERE a < '10.5'; -- { serverError TYPE_MISMATCH }
DROP TABLE bug_67742;

DROP TABLE IF EXISTS bug_67742;
CREATE TABLE bug_67742 (a Int32 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple();
INSERT INTO bug_67742 SELECT number FROM system.numbers LIMIT 10000;
SELECT count(*) FROM bug_67742 WHERE a < 10.5;
DROP TABLE bug_67742;

DROP TABLE IF EXISTS bug_67742;
CREATE TABLE bug_67742 (a Int16 STATISTICS(tdigest)) Engine = MergeTree() ORDER BY tuple();
INSERT INTO bug_67742 SELECT number FROM system.numbers LIMIT 10000;
SELECT count(*) FROM bug_67742 WHERE a < '9999999999999999999999999';
DROP TABLE bug_67742;
