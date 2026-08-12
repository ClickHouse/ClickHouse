-- Regression test for issue #80893: a constant LowCardinality aggregation key
-- lost its constness in the distributed merging-aggregator and arrived as
-- Const(LowCardinality(...)), aborting with a LOGICAL_ERROR in
-- HashMethodSingleLowCardinalityColumn. Must not crash.

-- QUALIFY is only supported by the analyzer; force it so the old-analyzer CI lane
-- runs the query instead of erroring with NOT_IMPLEMENTED.
SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;
-- Force the remote read path: prefer_localhost_replica defaults to 1, which makes
-- SelectStreamFactory pick a local stream for a local non-replicated table and
-- bypass the distributed merging-aggregator that #80893 crashed in.
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 LowCardinality(Bool)) ENGINE = Memory;
INSERT INTO t0 VALUES (TRUE);

SELECT c0 FROM remote('127.0.0.{1,2}', currentDatabase(), 't0') AS tx GROUP BY ALL QUALIFY c0;

DROP TABLE t0;
