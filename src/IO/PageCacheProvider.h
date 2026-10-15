#pragma once

#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <Common/PageCache.h>
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

/// `CacheReader` for one cached block: it holds the block's cell and serves `read` as a zero-copy view.
class PageCacheReader : public CacheReader
{
public:
    PageCacheReader(ByteRange range_in_file, PageCache::MappedPtr cell_);

    ByteRange range() const override { return range_member; }
    ChainedBuffers read(ByteRange sub) override;

private:
    ByteRange range_member;
    PageCache::MappedPtr cell;  /// the block's cell, kept alive by the shared_ptr
};

/// `CacheWriter` for one whole miss block, all-or-nothing. `write` creates the block's cell on demand
/// (`PageCache::getOrSet`, first-writer-wins) and adopts it; the writer then serves `read` for it. The
/// block is either committed (`cell` set) or not, so there is no committed-range set.
class PageCacheWriter : public CacheWriter
{
public:
    PageCacheWriter(
        PageCachePtr cache_,
        PageCacheFile file_,
        bool inject_eviction_,
        bool bypass_if_missing_,
        ByteRange block_range);

    ByteRange range() const override { return range_member; }
    size_t committed() const override
    {
        std::lock_guard lock(committed_mutex);
        /// Whole-segment: the block is either empty or fully committed.
        return cell ? range_member.end() : range_member.offset;
    }
    bool fillsWholeSegment() const override { return true; }
    size_t write(ChainedBuffers data, const FillRole & role) override;
    ChainedBuffers read(ByteRange sub) override;
    /// Re-probe the cache: if the block was cached by a concurrent query since `resolve`, adopt its cell
    /// (`committed()` then reports it) and hold no role; otherwise hold the role to fill it.
    FillRole takeFillRole() override;

private:
    PageCachePtr cache;
    PageCacheFile file;
    bool inject_eviction;
    /// Mirrors `read_from_page_cache_if_exists_otherwise_bypass_cache`: a
    /// bypass tier populates nothing, so `write` returns 0 before any `getOrSet`.
    bool bypass_if_missing;
    ByteRange range_member;  /// the one block this writer covers
    /// The block's cell once populated (by us) or adopted (a concurrent first-writer); null = not
    /// committed.
    PageCache::MappedPtr cell;
    /// Guards `cell`. Per the `CacheWriter::committed` contract - and like the disk cache's
    /// `committed_mutex` - a background prefetch and the foreground read may fill and read this writer
    /// at the same time.
    mutable std::mutex committed_mutex;
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

    /// A page-cache block is written whole (first-writer-wins, no later
    /// completion); the probe reports one miss range per block.
    bool fillsWholeSegment() const override { return true; }

    /// Resolve `range` into per-block hits (readers) and misses (whole-block
    /// writers when populating); see the definition.
    VectorWithMemoryTracking<CacheResolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) override;

private:
    PageCachePtr cache;
    PageCacheFile file;
    const size_t block_size;
    bool inject_eviction;
    bool bypass_if_missing;
    size_t file_size_in_bytes;
};

}
