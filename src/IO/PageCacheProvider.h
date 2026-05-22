#pragma once

#include <IO/ICacheProvider.h>
#include <Common/PageCache.h>
#include <Common/logger_useful.h>

namespace DB
{

/// RopeBuffer backed by a PageCache cell. Zero-copy: the shared_ptr pins the cell,
/// and data() points directly into the cache's mmap arena.
class PageCacheRopeBuffer : public RopeBuffer
{
public:
    explicit PageCacheRopeBuffer(PageCache::MappedPtr cell_)
        : cell(std::move(cell_))
    {
    }

    char * data() override { return cell->data(); }
    const char * data() const override { return cell->data(); }
    size_t size() const override { return cell->size(); }
    void transferTo(MemoryTracker * /* new_tracker */) override {}

private:
    PageCache::MappedPtr cell;
};


/// ICacheHandle for PageCache. Holds pinned cells for the lookup duration.
class PageCacheHandle : public ICacheHandle
{
public:
    PageCacheHandle(
        PageCacheFile file,
        ByteRange requested,
        PageCachePtr cache,
        size_t block_size,
        bool inject_eviction);

    CacheLookupResult status() const override;
    Rope get(ByteRange range) override;
    size_t put(ByteRange range, Rope data) override;

private:
    struct Block
    {
        PageCacheByteRange byte_range;
        UInt128 key_hash;
        PageCache::MappedPtr cell;  /// non-null for hits
        bool is_hit = false;
    };

    PageCacheFile file;
    PageCachePtr cache;
    bool inject_eviction;
    std::vector<Block> blocks;
    LoggerPtr log = getLogger("PageCacheHandle");
};


/// ICacheProvider wrapping PageCache.
class PageCacheProvider : public ICacheProvider
{
public:
    PageCacheProvider(PageCachePtr cache_, size_t block_size_, bool inject_eviction_)
        : cache(std::move(cache_))
        , block_size(block_size_)
        , inject_eviction(inject_eviction_)
    {
    }

    std::unique_ptr<ICacheHandle> lookup(CacheKey key, ByteRange range) override;
    String name() const override { return "PageCache"; }

private:
    PageCachePtr cache;
    size_t block_size;
    bool inject_eviction;
};

}
