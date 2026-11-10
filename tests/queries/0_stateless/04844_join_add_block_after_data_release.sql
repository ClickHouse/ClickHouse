-- Tags: no-fasttest, no-parallel
-- no-fasttest: needs a build with libfiu to enable the failpoint.
-- no-parallel: the failpoint is server-wide and fires once, so a concurrent copy of this test
-- consumes the trigger and this one then gets LIMIT_EXCEEDED instead of the injected fault.

SET join_algorithm = 'grace_hash';
SET max_bytes_in_join = 100000;
SET grace_hash_join_initial_buckets = 1;
-- With a single build thread the throw unwinds before anything re-enters the join, and the
-- query then reports the injected fault whether or not the fix is present.
SET max_threads = 4;

-- A throw while the join's data is released leaves the instance alive with no data, and the
-- rehash path then adds another block to it. Without the guard that read dereferences null.
SYSTEM ENABLE FAILPOINT hash_join_throw_after_data_release;

SELECT count() FROM (SELECT number AS k, toString(number) AS v FROM numbers(2000000)) AS l
    INNER JOIN (SELECT number AS k, toString(number) AS v FROM numbers(2000000)) AS r
    USING (k); -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT hash_join_throw_after_data_release;
