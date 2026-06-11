#pragma once

#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <Common/PageCache.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>

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


/// ── Per-range buffer API (see `ICacheProvider.h`) ──

/// Held, re-readable view of ONE resident (hit) file-level range. Owns the pinned
/// `PageCache::MappedPtr` cell(s) of the whole-block(s) backing the range, so the
/// bytes stay alive for the buffer's lifetime. Re-readable any sub-range, any
/// number of times; holds NO cursor and never mutates the cache. Page cells are
/// whole blocks, so `readable() == range().end()` (no partial-prefix growth), and
/// there is no deferred-LRU bump (page cache has no LRU touch on read). See
/// `CacheReader`.
class PageCacheReader : public CacheReader
{
public:
    struct HeldCell
    {
        PageCacheByteRange byte_range;
        PageCache::MappedPtr cell;
    };

    PageCacheReader(ByteRange range_in_file, std::vector<HeldCell> cells_)
        : range_member(range_in_file), cells(std::move(cells_))
    {
    }

    ByteRange range() const override { return range_member; }
    /// Page cells are whole blocks: the entire hit range is readable at once.
    size_t readable() const override { return range_member.end(); }
    Rope read(ByteRange sub) override;

private:
    ByteRange range_member;
    std::vector<HeldCell> cells;
};

/// Held, incrementally-fillable target for ONE miss file-level range. The cells of
/// the whole-block(s) backing the aligned range are created LAZILY on `write` (via
/// `PageCache::getOrSet`, first-writer-wins), adopted into `blocks`, and the page
/// write buffer DOUBLES as a read buffer — the executor can serve a self-populated
/// page block straight back from it. Page cache has no evictable in-flight segment,
/// so `pin` keeps the default no-op. See `CacheWriter`.
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
        ByteRange aligned_range_in_file)
        : cache(std::move(cache_))
        , file(std::move(file_))
        , block_size(block_size_)
        , file_size_in_bytes(file_size_in_bytes_)
        , inject_eviction(inject_eviction_)
        , bypass_if_missing(bypass_if_missing_)
        , range_member(aligned_range_in_file)
    {
    }

    ByteRange range() const override { return range_member; }
    const IntervalSet & committed() const override { return committed_ranges; }
    bool complete() const override;
    size_t write(Rope data) override;
    Rope read(ByteRange sub) override;

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
    /// Mirrors `read_from_page_cache_if_exists_otherwise_bypass_cache`: a bypass
    /// tier populates nothing, so `write` returns 0 before any `getOrSet`.
    bool bypass_if_missing;
    ByteRange range_member;
    IntervalSet committed_ranges;
    std::vector<AdoptedBlock> blocks;
    LoggerPtr log = getLogger("PageCacheWriter");
};

/// Read-only/lazy `CacheView` returned by `PageCacheProvider::planResidencyView`.
/// Holds the per-range hit read buffers (each pinning its block cells) and the
/// cache-aligned miss entries. No deferred-LRU bump (page cache has none), so no
/// special destructor. See `CacheView`.
class PageCacheView : public CacheView
{
public:
    const std::vector<HitEntry> & hits() const override { return hit_entries; }
    const std::vector<MissEntry> & misses() const override { return miss_entries; }

    std::vector<HitEntry> hit_entries;
    std::vector<MissEntry> miss_entries;
};


/// ICacheProvider wrapping PageCache.
///
/// PageCache is a FILE-level cache (one logical file per `PageCacheFile`
/// regardless of how many `StoredObject`s back it), so the `file` is
/// configured once at construction. `lookup` ignores the `StoredObject`
/// argument — multi-object gather mode still results in a single
/// PageCacheFile.
class PageCacheProvider : public ICacheProvider
{
public:
    /// `file_size_in_bytes` must be the authoritative byte length of the
    /// underlying file. The handle uses it to clamp the tail block's
    /// `PageCacheByteRange::size` to `min(block_size, file_size - offset)`,
    /// so the cache cell is allocated to its actual valid-byte length and
    /// has no past-EOF region that could be served on a future read.
    /// PageCache requires known size — sources with unknown size must not be
    /// wrapped in this provider (matches master's `CachedInMemoryReadBufferFromFile`,
    /// which calls `file_size.value()` everywhere).
    PageCacheProvider(
        PageCachePtr cache_,
        PageCacheFile file_,
        size_t block_size_,
        bool inject_eviction_,
        bool bypass_if_missing_,
        size_t file_size_in_bytes_)
        : cache(std::move(cache_))
        , file(std::move(file_))
        , block_size(block_size_)
        , inject_eviction(inject_eviction_)
        , bypass_if_missing(bypass_if_missing_)
        , file_size_in_bytes(file_size_in_bytes_)
    {
    }

    String name() const override { return "PageCache"; }
    CacheTier tier() const override { return CacheTier::PageCache; }
    bool populatesOnMiss() const override { return !bypass_if_missing; }

    /// A page-cache block is written whole (first-writer-wins, no later completion), so
    /// both edges round to `block_size`: the head fills the block this read starts in,
    /// the tail completes the block it ends in.
    size_t fetchHeadAlignment() const override { return block_size; }
    size_t fetchTailAlignment() const override { return block_size; }

    /// Read-only residency probe: hit read buffers + cache-aligned writer-null misses
    /// (writers come from `openWriteBuffers`).
    CacheViewPtr planResidencyView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) override;

    /// Open write buffers for the already-known whole-block-aligned miss ranges.
    /// The cells are created lazily on the first `write` of each block. Returns
    /// empty when `!populatesOnMiss()`. See `ICacheProvider::openWriteBuffers`.
    std::vector<MissEntry> openWriteBuffers(
        const StoredObject & object, size_t object_file_offset,
        const std::vector<ByteRange> & aligned_miss_ranges) override;

private:
    /// Backs `planResidencyView`: read-only block-by-block residency probe over
    /// `range_in_file`, coalescing adjacent hit blocks into one `HitEntry` and
    /// adjacent miss blocks into one `MissEntry` (writer null).
    CacheViewPtr buildView(ByteRange range_in_file);

    PageCachePtr cache;
    PageCacheFile file;
    size_t block_size;
    bool inject_eviction;
    bool bypass_if_missing;
    size_t file_size_in_bytes;
    LoggerPtr log = getLogger("PageCacheProvider");
};

}
