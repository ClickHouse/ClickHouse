-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: failpoint is used which can force DelayedSource on other tests
-- Tag no-fasttest: fasttest is built without SSL so remoteSecure() fails

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Memory;

SYSTEM ENABLE FAILPOINT use_delayed_remote_source;

SELECT count() FROM remoteSecure('localhost:9440', currentDatabase(), 't0') AS tx; -- { serverError 265 }

SYSTEM DISABLE FAILPOINT use_delayed_remote_source;

DROP TABLE t0;
