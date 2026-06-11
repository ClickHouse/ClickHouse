#include <IO/PageCacheProvider.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <algorithm>
#include <cstring>
#include <vector>

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
    /// Bypass mode (`read_from_page_cache_if_exists_otherwise_bypass_cache`) is a
    /// read-only probe: a miss must not populate the shared cache. Skip entirely -
    /// a detached cell is never registered and never read again (the executor
    /// already has the source bytes), so allocating/copying it is pure waste, and
    /// counting it as pushed-to-cache would be wrong.
    if (bypass_if_missing)
        return 0;

    size_t bytes_written = 0;
    for (auto & block : blocks)
    {
        if (block.is_hit)
            continue;

        ByteRange block_range{block.byte_range.offset, block.byte_range.size};

        if (block_range.end() <= range.offset || block_range.offset >= range.end())
            continue;

        /// First-writer-wins: if another thread cached this block concurrently,
        /// `getOrSet` returns the existing cell and does not call the load lambda.
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
                /// `data` must start at the block boundary and be contiguous:
                /// the cell expects block-relative layout, so a wrong start
                /// offset or an internal gap would poison it. Partial-at-end
                /// (EOF) is fine; leading/internal gaps throw.
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


Rope PageCacheReadBuffer::read(ByteRange sub)
{
    Rope result;

    /// Clamp `sub` to this buffer's own range first: a `read` for a `sub` outside
    /// `range_member` would otherwise reach into a neighbouring hit's cells. The
    /// contract is `sub` within `[range().offset, readable())`; clamp defensively,
    /// mirroring `DiskCacheReadBuffer::read`.
    {
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;
        sub = ByteRange{lo, hi - lo};
    }

    /// Build zero-copy `PageCacheRopeBuffer` nodes from the held cells overlapping
    /// `sub` — same body as the legacy `PageCacheHandle::get`.
    for (const auto & held : cells)
    {
        if (!held.cell)
            continue;

        ByteRange block_range{held.byte_range.offset, held.byte_range.size};
        if (block_range.end() <= sub.offset || block_range.offset >= sub.end())
            continue;

        size_t overlap_start = std::max(block_range.offset, sub.offset);
        size_t overlap_end = std::min(block_range.end(), sub.end());
        size_t offset_in_cell = overlap_start - block_range.offset;
        size_t overlap_size = overlap_end - overlap_start;

        auto buf = std::make_shared<PageCacheRopeBuffer>(held.cell);
        result.append(RopeNode{std::move(buf), offset_in_cell, overlap_size, overlap_start});
    }
    return result;
}


bool PageCacheWriteBuffer::complete() const
{
    /// `committed_ranges` covers the whole aligned range iff subtracting it leaves nothing.
    return committed_ranges.subtract(range_member).empty();
}

size_t PageCacheWriteBuffer::write(Rope data)
{
    /// Bypass mode is a read-only probe: a miss must not populate the shared cache.
    /// Skip entirely before any `getOrSet` — a bypass tier populates nothing.
    if (bypass_if_missing)
        return 0;

    SipHash base_hash = file.baseHash();

    size_t bytes_written = 0;
    /// Walk whole blocks of the aligned range; only act on those still uncommitted
    /// that `data` FULLY covers (the executor delivers block-aligned data, so a
    /// partially-covered block is left for a later `write`).
    for (size_t offset = range_member.offset; offset < range_member.end(); offset += block_size)
    {
        /// Tail block is clamped to the file's real byte length so the cell carries
        /// only valid bytes — same trick as the legacy `PageCacheHandle` ctor.
        size_t this_block_size = std::min(block_size, file_size_in_bytes - offset);
        ByteRange block_range{offset, this_block_size};

        /// Already committed (by us or a first-writer-wins peer adopted earlier) — skip.
        if (committed_ranges.subtract(block_range).empty())
            continue;

        /// Skip a block `data` does not fully cover; it is left for a later write.
        if (!data.covers(block_range))
            continue;

        PageCacheByteRange byte_range{block_range.offset, block_range.size};
        UInt128 key_hash = byte_range.hash(base_hash);

        /// First-writer-wins: if another thread cached this block concurrently,
        /// `getOrSet` returns the existing cell and does not call the load lambda.
        bool loaded = false;
        size_t loaded_bytes = 0;
        auto cell = cache->getOrSet(
            file,
            byte_range,
            /*detached_if_missing=*/false,
            inject_eviction,
            [&](const PageCache::MappedPtr & new_cell)
            {
                /// `data` must start at the block boundary and be contiguous: the
                /// cell expects block-relative layout, so a wrong start offset or an
                /// internal gap would poison it. Partial-at-end (EOF) is fine;
                /// leading/internal gaps throw. Same as the legacy `put`.
                Rope slice = data.slice(block_range);
                ByteRange covered = slice.range();
                size_t pos = covered.size;
                if (pos > 0)
                {
                    if (covered.offset != block_range.offset)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "PageCacheWriteBuffer::write: data does not start at block boundary: "
                            "block=[{}, {}), covered=[{}, {})",
                            block_range.offset, block_range.end(), covered.offset, covered.end());

                    ByteRange to_copy{block_range.offset, pos};
                    if (!slice.covers(to_copy))
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "PageCacheWriteBuffer::write: data has internal gaps within block [{}, {})",
                            block_range.offset, block_range.end());

                    slice.copyTo(new_cell->data(), to_copy);
                }

                if (pos < new_cell->size())
                    std::memset(new_cell->data() + pos, 0, new_cell->size() - pos);

                loaded = true;
                loaded_bytes = pos;
            },
            key_hash);

        /// ALWAYS adopt the returned cell (loaded by us OR an existing first-writer
        /// cell). The byte is now cached either way, so mark the block committed —
        /// otherwise `complete()` never becomes true under contention. Return only
        /// the bytes WE wrote (`loaded_bytes`); a first-writer-wins loss contributes
        /// 0 to the return but still advances `committed`.
        if (cell)
        {
            AdoptedBlock adopted;
            adopted.byte_range = byte_range;
            adopted.key_hash = key_hash;
            adopted.cell = cell;
            blocks.push_back(std::move(adopted));

            committed_ranges.add(block_range);

            if (loaded)
            {
                LOG_TRACE(log, "PageCacheWriteBuffer::write: populated block [{}, {})",
                    block_range.offset, block_range.end());
                bytes_written += loaded_bytes;
            }
        }
    }

    return bytes_written;
}

Rope PageCacheWriteBuffer::read(ByteRange sub)
{
    Rope result;

    /// Clamp to this buffer's range (its adopted cells only back `range_member`).
    {
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;
        sub = ByteRange{lo, hi - lo};
    }

    /// Serve from the adopted cells (self-populated page blocks) overlapping `sub`,
    /// zero-copy — the page write buffer doubles as a read buffer.
    for (const auto & block : blocks)
    {
        if (!block.cell)
            continue;

        ByteRange block_range{block.byte_range.offset, block.byte_range.size};
        if (block_range.end() <= sub.offset || block_range.offset >= sub.end())
            continue;

        size_t overlap_start = std::max(block_range.offset, sub.offset);
        size_t overlap_end = std::min(block_range.end(), sub.end());
        size_t offset_in_cell = overlap_start - block_range.offset;
        size_t overlap_size = overlap_end - overlap_start;

        auto buf = std::make_shared<PageCacheRopeBuffer>(block.cell);
        result.append(RopeNode{std::move(buf), offset_in_cell, overlap_size, overlap_start});
    }
    return result;
}


CacheViewPtr PageCacheProvider::buildView(ByteRange range_in_file)
{
    auto view = std::make_unique<PageCacheView>();

    /// Block-align the request; the tail block is clamped to the file's real byte
    /// length so a hit cell carries only valid bytes — same math as the legacy
    /// `PageCacheHandle` ctor.
    size_t aligned_start = (range_in_file.offset / block_size) * block_size;
    size_t aligned_end = ((range_in_file.end() + block_size - 1) / block_size) * block_size;
    if (aligned_end > file_size_in_bytes)
        aligned_end = ((file_size_in_bytes + block_size - 1) / block_size) * block_size;

    SipHash base_hash = file.baseHash();

    /// Accumulators for coalescing runs of adjacent same-kind blocks. A hit run
    /// collects its cells; a miss run only tracks the spanned range.
    std::vector<PageCacheReadBuffer::HeldCell> run_cells;
    ByteRange run_range{0, 0};
    bool run_active = false;
    bool run_is_hit = false;

    auto flush_run = [&]()
    {
        if (!run_active)
            return;
        if (run_is_hit)
        {
            auto reader = std::make_unique<PageCacheReadBuffer>(run_range, std::move(run_cells));
            view->hit_entries.push_back(HitEntry{run_range, std::move(reader)});
        }
        else
        {
            /// `planResidencyView`/`lookupView` never open writers; that is
            /// `openWriteBuffers`' job. Carry `writer == nullptr`.
            view->miss_entries.push_back(MissEntry{run_range, /*writer=*/nullptr});
        }
        run_cells.clear();
        run_active = false;
    };

    for (size_t offset = aligned_start; offset < aligned_end; offset += block_size)
    {
        size_t this_block_size = std::min(block_size, file_size_in_bytes - offset);
        PageCacheByteRange byte_range{offset, this_block_size};
        UInt128 key_hash = byte_range.hash(base_hash);
        ByteRange block_file{offset, this_block_size};

        /// Read-only probe — `cache->get` never creates a cell.
        auto cell = cache->get(key_hash, inject_eviction);
        const bool is_hit = (cell != nullptr);

        /// Break the run when the kind flips, then start/extend the current run.
        if (run_active && run_is_hit != is_hit)
            flush_run();

        if (!run_active)
        {
            run_active = true;
            run_is_hit = is_hit;
            run_range = block_file;
        }
        else
        {
            run_range.size = block_file.end() - run_range.offset;
        }

        if (is_hit)
            run_cells.push_back(PageCacheReadBuffer::HeldCell{byte_range, std::move(cell)});
    }
    flush_run();

    LOG_TRACE(log, "PageCacheProvider::buildView: file [{}, {}) → {} hits, {} misses",
        range_in_file.offset, range_in_file.end(), view->hit_entries.size(), view->miss_entries.size());

    return view;
}

CacheViewPtr PageCacheProvider::planResidencyView(
    const StoredObject & /*object*/, size_t /*object_file_offset*/, ByteRange range_in_file)
{
    return buildView(range_in_file);
}

std::vector<MissEntry> PageCacheProvider::openWriteBuffers(
    const StoredObject & /*object*/,
    size_t /*object_file_offset*/,
    const std::vector<ByteRange> & aligned_miss_ranges)
{
    /// A bypass tier populates nothing on miss, so it opens no writers.
    if (!populatesOnMiss())
        return {};

    std::vector<MissEntry> result;
    result.reserve(aligned_miss_ranges.size());

    /// PageCache is file-level — `object` / `object_file_offset` are ignored.
    /// Cells are created lazily on the first `write` of each block.
    for (const auto & aligned_file : aligned_miss_ranges)
    {
        auto writer = std::make_unique<PageCacheWriteBuffer>(
            cache,
            file,
            block_size,
            file_size_in_bytes,
            inject_eviction,
            bypass_if_missing,
            aligned_file);
        result.push_back(MissEntry{aligned_file, std::move(writer)});
    }

    return result;
}

}
