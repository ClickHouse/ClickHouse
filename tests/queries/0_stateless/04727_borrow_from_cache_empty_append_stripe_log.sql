-- Tags: no-fasttest, no-replicated-database

-- Regression test: an empty append on a `borrow_from_cache` / `memory` disk must not record a
-- phantom blob. `StripeLogSink` opens an append buffer before any rows are written and always
-- finalizes it, so `INSERT ... SELECT ... LIMIT 0` used to leave metadata pointing at a cache
-- segment that was never created, and a later read failed with `FILE_DOESNT_EXIST`.

-- The named `borrow_from_cache_disk` is defined in the server configuration: the direct-disk engines
-- accept only a named disk (not an inline definition), and a disk registered by an inline definition of
-- another table may be unknown when this table is loaded after a server restart.

DROP TABLE IF EXISTS tmp_stripe_log;
CREATE TABLE tmp_stripe_log (key UInt64, value String)
ENGINE = StripeLog
SETTINGS disk = 'borrow_from_cache_disk';

-- Empty append into a fresh table: the data files are created but must reference no blob.
INSERT INTO tmp_stripe_log SELECT number, toString(number) FROM numbers(10) LIMIT 0;
SELECT count() FROM tmp_stripe_log;

-- Real data still works after the empty append.
INSERT INTO tmp_stripe_log VALUES (1, 'hello'), (2, 'world');
SELECT * FROM tmp_stripe_log ORDER BY key;

-- Empty append after real data: reading back must not hit a phantom blob at the end of the file.
INSERT INTO tmp_stripe_log SELECT number, toString(number) FROM numbers(10) LIMIT 0;
SELECT * FROM tmp_stripe_log ORDER BY key;

INSERT INTO tmp_stripe_log VALUES (3, 'again');
SELECT count() FROM tmp_stripe_log;

-- Clean up
DROP TABLE tmp_stripe_log;
