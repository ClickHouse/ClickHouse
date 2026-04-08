-- Tags: no-debug
-- no-debug: triggers a LOGICAL_ERROR

-- A test for bug 89166

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 ENGINE = Memory AS SELECT 1 c0;

-- This is a bug and must not throw
SELECT 1 FROM t0 tx WHERE c0 = 1 AND c0 = EXISTS(SELECT c0 = 1 FROM numbers(1)); -- { serverError LOGICAL_ERROR }

DROP TABLE t0;
