-- Tags: no-fasttest, no-parallel
-- Tag no-parallel: `SYSTEM SYNC FILE CACHE` performs a host-wide `sync` syscall that
-- flushes the OS page cache for the whole machine, not something scoped to this test.
-- no-fasttest: Will perform 'sync' syscall (it can take time)
system sync file cache;
