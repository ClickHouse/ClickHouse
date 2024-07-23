-- Tags: no-fasttest, no-parallel
-- no-fasttest because we need an S3 storage policy
-- no-parallel because we look at server-wide counters about page cache usage

set use_page_cache_for_disks_without_file_cache = 1;
set page_cache_inject_eviction = 0;
set enable_filesystem_cache = 0;
set use_uncompressed_cache = 0;

create table events_snapshot engine Memory as select * from system.events;
create view events_diff as
    -- round all stats to 70 MiB to leave a lot of leeway for overhead
    with if(event like '%Bytes%', 70*1024*1024, 35) as granularity,
    -- cache hits counter can vary a lot depending on other settings:
    -- e.g. if merge_tree_min_bytes_for_concurrent_read is small, multiple threads will read each chunk
    -- so we just check that the value is not too low
         if(event in (
            'PageCacheBytesUnpinnedRoundedToPages', 'PageCacheBytesUnpinnedRoundedToHugePages',
            'PageCacheChunkDataHits'), 1, 1000) as clamp
    select event, min2(intDiv(new.value - old.value, granularity), clamp) as diff
    from system.events new
    left outer join events_snapshot old
    on old.event = new.event
    where diff != 0 and
          event in (
            'ReadBufferFromS3Bytes', 'PageCacheChunkMisses', 'PageCacheChunkDataMisses',
            'PageCacheChunkDataHits', 'PageCacheChunkDataPartialHits',
            'PageCacheBytesUnpinnedRoundedToPages', 'PageCacheBytesUnpinnedRoundedToHugePages')
    order by event;

drop table if exists page_cache_03055;
create table page_cache_03055 (k Int64 CODEC(NONE)) engine MergeTree order by k settings storage_policy = 's3_cache';

-- Write an 80 MiB file (40 x 2 MiB chunks), and a few small files.
system stop merges page_cache_03055;
insert into page_cache_03055 select * from numbers(10485760) settings max_block_size=100000000, preferred_block_size_bytes=1000000000;

select * from events_diff;
truncate table events_snapshot;
insert into events_snapshot select * from system.events;

system start merges page_cache_03055;
optimize table page_cache_03055 final;
truncate table events_snapshot;
insert into events_snapshot select * from system.events;

-- Cold read, should miss cache. (Populating cache on write is not implemented yet.)

select 'cold read', sum(k) from page_cache_03055;

select * from events_diff where event not in ('PageCacheChunkDataHits');
truncate table events_snapshot;
insert into events_snapshot select * from system.events;

-- Repeat read, should hit cache.

select 'repeat read 1', sum(k) from page_cache_03055;

select * from events_diff;
truncate table events_snapshot;
insert into events_snapshot select * from system.events;

-- Drop cache and read again, should miss. Also don't write to cache.

system drop page cache;

select 'dropped and bypassed cache', sum(k) from page_cache_03055 settings read_from_page_cache_if_exists_otherwise_bypass_cache = 1;

-- Data could be read multiple times because we're not writing to cache.
-- (Not checking PageCacheBytesUnpinned* because it's unreliable in this case because of an intentional race condition, see PageCache::evictChunk.)
select event, if(event in ('PageCacheChunkMisses', 'ReadBufferFromS3Bytes'), diff >= 1, diff) from events_diff where event not in ('PageCacheChunkDataHits', 'PageCacheBytesUnpinnedRoundedToPages', 'PageCacheBytesUnpinnedRoundedToHugePages');
truncate table events_snapshot;
insert into events_snapshot select * from system.events;

-- Repeat read, should still miss, but populate cache.

select 'repeat read 2', sum(k) from page_cache_03055;

select * from events_diff where event not in ('PageCacheChunkDataHits');
truncate table events_snapshot;
insert into events_snapshot select * from system.events;

-- Read again, hit the cache.

select 'repeat read 3', sum(k) from page_cache_03055 settings read_from_page_cache_if_exists_otherwise_bypass_cache = 1;

select * from events_diff;
truncate table events_snapshot;
insert into events_snapshot select * from system.events;


-- Known limitation: cache is not invalidated if a table is dropped and created again at the same path.
-- set allow_deprecated_database_ordinary=1;
-- create database test_03055 engine = Ordinary;
-- create table test_03055.t (k Int64) engine MergeTree order by k settings storage_policy = 's3_cache';
-- insert into test_03055.t values (1);
-- select * from test_03055.t;
-- drop table test_03055.t;
-- create table test_03055.t (k Int64) engine MergeTree order by k settings storage_policy = 's3_cache';
-- insert into test_03055.t values (2);
-- select * from test_03055.t;


drop table events_snapshot;
drop table page_cache_03055;
drop view events_diff;
