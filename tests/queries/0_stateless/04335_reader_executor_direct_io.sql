-- Tags: no-fasttest, no-object-storage, no-random-settings
-- no-fasttest: experimental ReaderExecutor is not exercised reliably there.
-- no-object-storage: this guards the LOCAL read path; an object-storage disk
-- would not use a local file descriptor at all.
-- no-random-settings: the test pins min_bytes_to_use_direct_io / the local read
-- method to drive the executor's local source through a synchronous O_DIRECT-capable
-- descriptor; random settings would override exactly those knobs.

-- Reading a wide-part column through the ReaderExecutor with O_DIRECT selected must
-- succeed. The executor read source bytes in external-buffer mode (set()) straight
-- into rope memory, which is not sector-aligned, so an O_DIRECT pread would fail with
-- EINVAL. An aligned descriptor now opts out of external-buffer mode and the executor
-- copies through its aligned internal buffer instead. (On a filesystem without strict
-- O_DIRECT support the read falls back to buffered, but this still guards the
-- executor's local read path end-to-end.)

SET use_reader_executor = 1;
SET min_bytes_to_use_direct_io = 1;
SET local_filesystem_read_method = 'pread';

DROP TABLE IF EXISTS t_reader_executor_direct_io;
CREATE TABLE t_reader_executor_direct_io (id UInt64, v String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Incompressible values so the column file is large enough to actually use O_DIRECT.
INSERT INTO t_reader_executor_direct_io
SELECT number, randomPrintableASCII(200) FROM numbers(200000);

-- NOT ignore(v) forces reading the v data substream (not just its sizes), which is the
-- large read driven through the executor; it is always true, so this is the row count.
SELECT count() FROM t_reader_executor_direct_io WHERE NOT ignore(v);

DROP TABLE t_reader_executor_direct_io;
