#pragma once

#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <Common/PageCache.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>

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

/// `CacheWriter` for one miss range. `write` creates each block's cell on demand
/// (`PageCache::getOrSet`, first-writer-wins) and adopts it into `blocks`, which also lets the writer
/// serve `read` for the blocks it populated.
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
    IntervalSet committed() const override { return committed_ranges; }
    size_t write(ChainedBuffers data, const Claim & claim) override;
    ChainedBuffers read(ByteRange sub) override;
    /// Re-probe the cache: a block may have been populated by a concurrent query since `resolve`. Any
    /// resident prefix is adopted and reported as `available` (already committed, served from cache),
    /// so the executor does not re-read it from the source.
    Lead claimLeadRole(ByteRange range) override;

private:
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
    /// The whole-block cells this writer populated or adopted, in file order (same layout as the
    /// reader's `cell`: each carries its own `cell->range` and size). One writer is driven by a single
    /// thread, so no lock is needed.
    VectorWithMemoryTracking<PageCache::MappedPtr> blocks;
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
    bool fillsWholeSegment() const override { return true; }

    /// Resolve `range` into per-block hits (readers) and misses (whole-block
    /// writers when populating); see the definition.
    VectorWithMemoryTracking<CacheResolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) override;

private:
    PageCachePtr cache;
    PageCacheFile file;
    size_t block_size;
    bool inject_eviction;
    bool bypass_if_missing;
    size_t file_size_in_bytes;
    LoggerPtr log = getLogger("PageCacheProvider");
};

}
