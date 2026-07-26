#pragma once

#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <Common/PageCache.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>

#include <mutex>

namespace DB
{

/// ChainedBuffer backed by a PageCache cell. Zero-copy: the shared_ptr pins the
/// cell, and data() points directly into the cache's mmap arena.
class PageCacheChainedBuffer : public ChainedBuffer
{
public:
    explicit PageCacheChainedBuffer(PageCache::MappedPtr cell_);

    char * data() override { return cell->data(); }
    const char * data() const override { return cell->data(); }
    size_t size() const override { return cell->size(); }

private:
    PageCache::MappedPtr cell;
};


/// ── Per-range buffer API (see `ICacheProvider.h`) ──

/// `CacheReader` over the pinned whole-block cell(s) backing one hit range.
/// Page cells are whole blocks, so a hit is always fully resident and there is
/// no deferred-LRU bump.
class PageCacheReader : public CacheReader
{
public:
    struct HeldCell
    {
        PageCacheByteRange byte_range;
        PageCache::MappedPtr cell;
    };

    PageCacheReader(ByteRange range_in_file, VectorWithMemoryTracking<HeldCell> cells_);

    ByteRange range() const override { return range_member; }
    ChainedBuffers read(ByteRange sub) override;

private:
    ByteRange range_member;
    VectorWithMemoryTracking<HeldCell> cells;
};

/// `CacheWriter` over one whole-block-aligned miss range. Cells are created
/// lazily on `write` (`PageCache::getOrSet`, first-writer-wins) and adopted
/// into `blocks`; the write buffer doubles as a read buffer for the
/// self-populated blocks. No evictable in-flight segment, so `pin` keeps the
/// default no-op.
class PageCacheWriter : public CacheWriter
{
public:
    PageCacheWriter(
        PageCachePtr cache_,
        PageCacheFile file_,
        size_t block_size_,
        size_t file_size_in_bytes_,
        bool inject_eviction_,
        bool bypass_if_missing_,
        ByteRange aligned_range_in_file);

    ByteRange range() const override { return range_member; }
    IntervalSet committed() const override
    {
        std::lock_guard lock(state_mutex);
        return committed_ranges;
    }
    size_t write(ChainedBuffers data) override;
    ChainedBuffers read(ByteRange sub) override;

private:
    struct AdoptedBlock
    {
        PageCacheByteRange byte_range;
        UInt128 key_hash{};
        PageCache::MappedPtr cell;
    };

    PageCachePtr cache;
    PageCacheFile file;
    size_t block_size;
    size_t file_size_in_bytes;
    bool inject_eviction;
    /// Mirrors `read_from_page_cache_if_exists_otherwise_bypass_cache`: a
    /// bypass tier populates nothing, so `write` returns 0 before any `getOrSet`.
    bool bypass_if_missing;
    ByteRange range_member;
    IntervalSet committed_ranges;
    VectorWithMemoryTracking<AdoptedBlock> blocks;
    /// Guards `committed_ranges` and `blocks`: the prefetch worker may write this writer
    /// while the foreground reads the self-populated blocks of the same writer.
    mutable std::mutex state_mutex;
    LoggerPtr log = getLogger("PageCacheWriter");
};

/// `ICacheProvider` wrapping PageCache. PageCache is FILE-level (one logical
/// file per `PageCacheFile` regardless of how many `StoredObject`s back it),
/// so the file is configured once at construction and lookups ignore the
/// `StoredObject` argument.
class PageCacheProvider : public ICacheProvider
{
public:
    /// `file_size_in_bytes` must be the authoritative byte length: tail cells
    /// are clamped to it so no past-EOF region can be served later. PageCache
    /// requires known size - unknown-size sources must not be wrapped.
    PageCacheProvider(
        PageCachePtr cache_,
        PageCacheFile file_,
        size_t block_size_,
        bool inject_eviction_,
        bool bypass_if_missing_,
        size_t file_size_in_bytes_);

    String name() const override { return "PageCache"; }
    CacheTier tier() const override { return CacheTier::PageCache; }
    bool populatesOnMiss() const override { return !bypass_if_missing; }

    /// A page-cache block is written whole (first-writer-wins, no later
    /// completion); the probe reports one miss range per block.
    bool fillsWholeCell() const override { return true; }

    std::unique_ptr<IProbeCursor> probe() override;

    /// Cells are created lazily on the first `write` of each block.
    CacheWriterPtr openWriter(
        const StoredObject & object, size_t object_file_offset, ByteRange cell) override;

private:
    /// Defined in the .cpp; nested for private-member access.
    class ProbeCursor;
    /// The step probe body (the cursor delegates; stateless per step).
    Resolution resolve(const StoredObject & object, size_t object_file_offset, size_t pos_in_file);

    PageCachePtr cache;
    PageCacheFile file;
    size_t block_size;
    bool inject_eviction;
    bool bypass_if_missing;
    size_t file_size_in_bytes;
    LoggerPtr log = getLogger("PageCacheProvider");
};

}
