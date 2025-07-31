-- Tags: no-parallel
-- Tag no-parallel: failpoint is used which can force DelayedSource on other tests

DROP TABLE IF EXISTS 02863_delayed_source;

CREATE TABLE 02863_delayed_source(a Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/02863_delayed_source/{replica}', 'r1') ORDER BY a;
INSERT INTO 02863_delayed_source VALUES (1), (2);

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Memory;

SYSTEM ENABLE FAILPOINT use_delayed_remote_source;

SELECT sum(a) FROM remote('127.0.0.4', currentDatabase(), '02863_delayed_source') WITH TOTALS SETTINGS extremes = 1;
SELECT max(explain like '%Delayed%') FROM (EXPLAIN PIPELINE graph=1 SELECT sum(a) FROM remote('127.0.0.4', currentDatabase(), '02863_delayed_source') WITH TOTALS SETTINGS extremes = 1);
SELECT sum(a) FROM remote('127.0.0.4', currentDatabase(), '02863_delayed_source') GROUP BY a ORDER BY a LIMIT 1 FORMAT JSON settings output_format_write_statistics=0;

-- issue #83282, should error gracefully
SELECT count() FROM remoteSecure('127.0.0.4', currentDatabase(), 't0') AS tx; -- { serverError 519 }

SYSTEM DISABLE FAILPOINT use_delayed_remote_source;

DROP TABLE 02863_delayed_source;
DROP TABLE t0;
