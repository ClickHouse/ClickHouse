#include <IO/PageCacheProvider.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PageCacheHandle::PageCacheHandle(
    PageCacheFile file_,
    ByteRange requested,
    PageCachePtr cache_,
    size_t block_size,
    bool inject_eviction_,
    bool bypass_if_missing_,
    size_t file_size_in_bytes)
    : file(std::move(file_))
    , cache(std::move(cache_))
    , inject_eviction(inject_eviction_)
    , bypass_if_missing(bypass_if_missing_)
{
    /// Split the requested range into block-aligned chunks. The tail block's
    /// size is clamped to the file's actual byte length so the cell carries
    /// only valid bytes — same trick as `CachedInMemoryReadBufferFromFile`
    /// (`min(block_size, file_size - cache_range.offset)`). With cells sized
    /// to their real-data length, the cell has no past-EOF region for `get`
    /// to ever serve, and `put` can't leave a zero-filled trailing gap that
    /// a later reader could mistake for file content.
    size_t aligned_start = (requested.offset / block_size) * block_size;
    size_t aligned_end = ((requested.end() + block_size - 1) / block_size) * block_size;
    if (aligned_end > file_size_in_bytes)
        aligned_end = ((file_size_in_bytes + block_size - 1) / block_size) * block_size;

    SipHash base_hash = file.baseHash();

    for (size_t offset = aligned_start; offset < aligned_end; offset += block_size)
    {
        size_t this_block_size = std::min(block_size, file_size_in_bytes - offset);
        PageCacheByteRange byte_range{offset, this_block_size};
        UInt128 key_hash = byte_range.hash(base_hash);

        auto cell = cache->get(key_hash, inject_eviction);

        Block block;
        block.byte_range = byte_range;
        block.key_hash = key_hash;
        block.cell = std::move(cell);
        block.is_hit = (block.cell != nullptr);
        blocks.push_back(std::move(block));
    }

    LOG_TRACE(log, "PageCacheHandle: requested [{}, {}), {} blocks, {} hits",
        requested.offset, requested.end(), blocks.size(),
        std::count_if(blocks.begin(), blocks.end(), [](const Block & b) { return b.is_hit; }));
}

CacheLookupResult PageCacheHandle::status() const
{
    CacheLookupResult result;
    for (const auto & block : blocks)
    {
        ByteRange r{block.byte_range.offset, block.byte_range.size};
        if (block.is_hit)
            result.hit_ranges.push_back(r);
        else
            result.miss_ranges.push_back(r);
    }
    return result;
}

Rope PageCacheHandle::get(ByteRange range)
{
    Rope result;
    for (const auto & block : blocks)
    {
        if (!block.is_hit || !block.cell)
            continue;

        ByteRange block_range{block.byte_range.offset, block.byte_range.size};

        /// Check overlap with requested range.
        if (block_range.end() <= range.offset || block_range.offset >= range.end())
            continue;

        size_t overlap_start = std::max(block_range.offset, range.offset);
        size_t overlap_end = std::min(block_range.end(), range.end());
        size_t offset_in_cell = overlap_start - block_range.offset;
        size_t overlap_size = overlap_end - overlap_start;

        auto buf = std::make_shared<PageCacheRopeBuffer>(block.cell);
        result.append(RopeNode{std::move(buf), offset_in_cell, overlap_size, overlap_start});
    }
    return result;
}

size_t PageCacheHandle::put(ByteRange range, Rope data)
{
    size_t bytes_written = 0;
    for (auto & block : blocks)
    {
        if (block.is_hit)
            continue;

        ByteRange block_range{block.byte_range.offset, block.byte_range.size};

        /// Check if the provided data covers this miss block.
        if (block_range.end() <= range.offset || block_range.offset >= range.end())
            continue;

        /// Use getOrSet with a load lambda that copies from the provided Rope.
        /// First-writer-wins: if another thread cached this block concurrently,
        /// getOrSet returns the existing cell and doesn't call load.
        bool loaded = false;
        size_t loaded_bytes = 0;
        /// `detached_if_missing` (= `bypass_if_missing`) makes
        /// `getOrSet` allocate a standalone cell that is NOT registered
        /// with the cache. The load lambda still fills it, the handle
        /// still serves it via `get`, but it's invisible to other
        /// queries — matching the
        /// `read_from_page_cache_if_exists_otherwise_bypass_cache`
        /// semantics (read-only probe for cache; populate transient cell
        /// for this read only).
        auto cell = cache->getOrSet(
            file,
            block.byte_range,
            /*detached_if_missing=*/bypass_if_missing,
            inject_eviction,
            [&](const PageCache::MappedPtr & new_cell)
            {
                /// `Rope::copyTo(dst, req)` writes bytes at `dst[lo - req.offset]`
                /// for each byte at logical offset `lo`. The cell expects
                /// block-relative layout `cell[lo - block_range.offset]`, so
                /// `req.offset` must equal `block_range.offset` — otherwise the
                /// bytes land at the wrong cell offset and a later `get` would
                /// serve corrupted data.
                ///
                /// The executor contracts that `data` starts at the block
                /// boundary and is internally contiguous: each `put` is fed by
                /// a single source read covering exactly one miss range, which
                /// `status()` reports as a full block. Partial-at-end
                /// (`covered.size < block_range.size`, e.g. EOF) is allowed;
                /// leading or internal gaps are not. Throw rather than silently
                /// poison the cache when the contract is violated.
                Rope slice = data.slice(block_range);
                ByteRange covered = slice.range();
                size_t pos = covered.size;
                if (pos > 0)
                {
                    if (covered.offset != block_range.offset)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "PageCacheHandle::put: data does not start at block boundary: "
                            "block=[{}, {}), covered=[{}, {})",
                            block_range.offset, block_range.end(), covered.offset, covered.end());

                    ByteRange to_copy{block_range.offset, pos};
                    if (!slice.covers(to_copy))
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "PageCacheHandle::put: data has internal gaps within block [{}, {})",
                            block_range.offset, block_range.end());

                    slice.copyTo(new_cell->data(), to_copy);
                }

                /// File-size-aware cell sizing in the ctor normally makes
                /// `pos == new_cell->size()`. The zero-fill below defends
                /// against unexpected short coverage so the cell never
                /// returns pre-allocation buffer bytes.
                if (pos < new_cell->size())
                    std::memset(new_cell->data() + pos, 0, new_cell->size() - pos);

                loaded = true;
                loaded_bytes = pos;
            },
            block.key_hash);

        if (loaded)
        {
            LOG_TRACE(log, "PageCacheHandle::put: populated block [{}, {})",
                block.byte_range.offset, block.byte_range.offset + block.byte_range.size);
            bytes_written += loaded_bytes;
        }

        /// Update the block state so subsequent get() calls work.
        block.cell = std::move(cell);
        block.is_hit = true;
    }

    return bytes_written;
}


std::unique_ptr<ICacheHandle> PageCacheProvider::lookup(
    const StoredObject & /*object*/,
    size_t /*object_file_offset*/,
    ByteRange range_in_file)
{
    /// PageCache is configured at the file level (one `PageCacheFile` for
    /// the entire `ReadPipeline`), so the per-object identity is
    /// irrelevant. `range_in_file` is already in the file-level coordinate
    /// space the `PageCacheHandle` uses internally — pass it through
    /// unchanged.
    return std::make_unique<PageCacheHandle>(
        file, range_in_file, cache, block_size, inject_eviction, bypass_if_missing, file_size_in_bytes);
}

}
