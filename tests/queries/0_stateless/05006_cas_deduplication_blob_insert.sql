-- Tags: long
-- On a content-addressed S3 disk (the cas_s3 test lane), byte-identical column
-- blobs deduplicate to a single object, so the second column's conditional PUT (If-None-Match: *) loses
-- its precondition. Before the `Expect: 100-continue` fix the rejected large body triggered a
-- 500/broken-pipe retry storm in the S3 client and this INSERT hung for tens of minutes (B118).
-- Regression: the INSERT must complete and the data must round-trip. On non-CA storage this is a
-- trivial fast insert.

DROP TABLE IF EXISTS t_cas_deduplicated_blob;

CREATE TABLE t_cas_deduplicated_blob (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;

-- x and y are byte-identical -> same content hash -> the second blob's conditional PUT 412s.
INSERT INTO t_cas_deduplicated_blob SELECT number, number FROM numbers(1000000);

SELECT count(), sum(x), sum(y), sum(x = y) FROM t_cas_deduplicated_blob;

DROP TABLE t_cas_deduplicated_blob;
