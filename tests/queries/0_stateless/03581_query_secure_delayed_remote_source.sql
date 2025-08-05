-- Tags: no-parallel
-- Tag no-parallel: failpoint is used which can force DelayedSource on other tests

SYSTEM ENABLE FAILPOINT use_delayed_remote_source;
SELECT count() FROM remoteSecure('localhost');
