-- `DETACH TABLE ... IF EMPTY` of a wrapper that owns inner tables only detaches the wrapper and
-- leaves the rows of its inner tables in place, so it is not judged by their row count.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS plain;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
INSERT INTO src VALUES (1);

CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES ('metric', map(), [(now64(3), 1.0)]);

CREATE TABLE plain (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO plain VALUES (1);

DETACH TABLE IF EMPTY mv;
DETACH TABLE IF EMPTY ts;
DETACH TABLE IF EMPTY plain; -- { serverError TABLE_NOT_EMPTY }

SELECT name FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE '.inner%' ORDER BY name;

ATTACH TABLE mv;
ATTACH TABLE ts;

SELECT count() FROM mv;
-- Parallel replicas over the non-replicated inner table would count each row once per replica.
SELECT count() FROM timeSeriesData(currentDatabase(), ts) SETTINGS enable_parallel_replicas = 0;

DROP TABLE mv;
DROP TABLE ts;
DROP TABLE plain;
DROP TABLE src;
