-- Tags: no-fasttest, no-distributed-cache, no-parallel-replicas
--   no-fasttest: needs minio (object storage).
--   no-distributed-cache: that stage substitutes its own seek threshold, so the counter below stops
--     describing the read this test measures.
--   no-parallel-replicas: distributed reading does the remote work under secondary queries, so the
--     measured query reports no remote bytes at all and the assertions would pass vacuously. The tag
--     only covers the runner's own parallel-replica runs, so the measured queries also pin
--     `enable_parallel_replicas = 0`: the stress runner enables it as a client option, which the tag
--     does not suppress.

-- A packed part stores every column file inside one `data.packed` archive. Reading a small marks
-- member sizes the buffer for that member, but the remote seek threshold still described the whole
-- session, so skipping the archive prefix ahead of the member was judged "too short to seek" and was
-- read-and-discarded through the member-sized buffer instead.
--
-- `body.cmrk2` is small (~500 bytes at the default geometry; its exact size follows the randomized
-- `index_granularity` and `compress_marks`) and sits behind a ~2.9 MiB prefix, which is below the
-- default `remote_read_min_bytes_for_seek` of 4 MiB, so the whole prefix used to be transferred to
-- obtain it.
--
-- The reported failure was a text-index query, but the amplification is in the archive-member read and
-- is independent of which index selected the granule, so this fixture uses a plain point lookup.
--
-- The disk is uncached on purpose. The discarded prefix is then counted where it is actually paid for,
-- on the object-storage read, and the measurement no longer depends on whether a shared filesystem
-- cache still holds the segment: an earlier revision counted local cache-file reads instead and was
-- flaky because any other query on the server could evict that segment between the warm-up and the
-- measured read. Encrypted because reading the encryption header initialises the gather buffer, which
-- is what puts the member seek on the branch under test.

-- Each measured read gets its own table. The two reads request the same bytes, so sharing one table
-- lets whichever runs first serve part of the second one from a read cache and halve its counter:
-- with `use_uncompressed_cache = 1` the second read measured 12398 bytes against 24796 on separate
-- tables, because `CachedCompressedReadBuffer` keys on (path, offset) and both reads use the same
-- path. That setting is randomized, so the second cell has to be measured on bytes nothing else has
-- already fetched.
DROP TABLE IF EXISTS t_packed_seek;
DROP TABLE IF EXISTS t_packed_seek_small_threshold;

CREATE TABLE t_packed_seek (id UInt64, big String, body String)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_cache_encrypted',
         -- Wide + Packed: the archive indirection under test. `min_bytes_for_full_part_storage`
         -- selects Packed for parts SMALLER than it, so a large value forces Packed here.
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 1000000000,
         -- Keep the skip-index members in the archive rather than spilling them out.
         packed_skip_index_max_bytes = 104857600,
         -- Both of the next settings are randomized, and either value alone moves `body.cmrk2` to the
         -- front of the archive, leaving no prefix to skip and making the assertions below pass
         -- without the bug being exercised at all. The separate size stream is what places the marks
         -- member after `big.bin`.
         string_serialization_version = 'with_size_stream',
         serialization_info_version = 'with_types',
         -- The measured query has to be the one that first reads these marks.
         prewarm_mark_cache = 0;

CREATE TABLE t_packed_seek_small_threshold AS t_packed_seek;

-- `randomPrintableASCII` is incompressible on purpose: a compressible filler (repeat('x', N)) shrinks
-- the archive to a few KB and leaves no prefix to skip, which makes the whole test vacuous.
INSERT INTO t_packed_seek
SELECT number, randomPrintableASCII(1000), 'vector word here' FROM numbers(3000);

INSERT INTO t_packed_seek_small_threshold
SELECT number, randomPrintableASCII(1000), 'vector word here' FROM numbers(3000);

-- Reads `body`, whose marks member sits at the far end of the archive.
SELECT body FROM t_packed_seek WHERE id = 2999
SETTINGS
         -- Selects the synchronous gather path, which is the one the counters below describe. Under
         -- `threadpool` the read goes through `AsynchronousBoundedReadBuffer`, which makes its own
         -- lazy-ignore decision from the same threshold, so the measurement would no longer be about
         -- the gather.
         remote_filesystem_read_method = 'read',
         -- Under parallel replicas the remote reads happen in secondary queries, so this query would
         -- record no remote bytes at all.
         enable_parallel_replicas = 0,
         remote_read_min_bytes_for_seek = 4194304,
         log_comment = '04654_far_member'
FORMAT Null;

-- Same read with a threshold far BELOW the buffer size: a guard that a small user value does not make
-- the transfer blow up. It does not distinguish taking a minimum from overwriting the value, because
-- either way the prefix here stays above both candidates and is seeked over.
SELECT body FROM t_packed_seek_small_threshold WHERE id = 2999
SETTINGS remote_filesystem_read_method = 'read',
         enable_parallel_replicas = 0,
         remote_read_min_bytes_for_seek = 128,
         log_comment = '04654_small_threshold'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Before the fix this fetched the whole prefix from object storage to obtain a few hundred bytes of
-- marks; now the prefix is seeked over. One example measurement is 27522 bytes after the fix against
-- 3049387 before it, and across the randomized geometry the two states stayed two orders of magnitude
-- apart with no run-to-run variance on either side.
--
-- The lower bound is a premise check rather than a property of the fix: the marks still have to be
-- fetched, so a near-zero reading means the read no longer reaches object storage and this fixture is
-- measuring something else. That fails loudly instead of passing while asserting nothing.
--
-- The request bound is deliberately generous (measured 9 before the fix and 11 to 12 after it, across
-- every geometry tried). Seeking rather than bridging a gap costs an extra request, which is the point
-- of the trade, but it must stay a small constant: it exists so that saving bytes by splitting the
-- read into many tiny requests cannot pass.
SELECT
    ProfileEvents['ReadBufferFromS3Bytes'] BETWEEN 5000 AND 1000000 AS remote_bytes_are_small,
    ProfileEvents['DiskS3GetObject'] < 100 AS requests_stay_bounded
FROM system.query_log
WHERE log_comment = '04654_far_member' AND type = 'QueryFinish'
  AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT
    ProfileEvents['ReadBufferFromS3Bytes'] BETWEEN 5000 AND 1000000 AS small_threshold_stays_small
FROM system.query_log
WHERE log_comment = '04654_small_threshold' AND type = 'QueryFinish'
  AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_packed_seek;
DROP TABLE t_packed_seek_small_threshold;
