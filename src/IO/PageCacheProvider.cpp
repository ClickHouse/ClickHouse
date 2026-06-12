#include <IO/PageCacheProvider.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <algorithm>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


PageCacheRopeBuffer::PageCacheRopeBuffer(PageCache::MappedPtr cell_)
    : cell(std::move(cell_))
{
}


PageCacheReader::PageCacheReader(ByteRange range_in_file, VectorWithMemoryTracking<HeldCell> cells_)
    : range_member(range_in_file)
    , cells(std::move(cells_))
{
}

Rope PageCacheReader::read(ByteRange sub)
{
    Rope result;

    /// Clamp `sub` to this buffer's own range: a `read` outside `range_member`
    /// would otherwise reach into a neighbouring hit's cells.
    {
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;
        sub = ByteRange{lo, hi - lo};
    }

    /// Zero-copy nodes from the held cells overlapping `sub`.
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


PageCacheWriter::PageCacheWriter(
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

bool PageCacheWriter::complete() const
{
    return committed_ranges.subtract(range_member).empty();
}

size_t PageCacheWriter::write(Rope data)
{
    /// A bypass tier populates nothing - skip before any `getOrSet`.
    if (bypass_if_missing)
        return 0;

    SipHash base_hash = file.baseHash();

    size_t bytes_written = 0;
    /// Walk whole blocks of the aligned range; only act on uncommitted blocks
    /// that `data` FULLY covers (a partially-covered block is left for a later
    /// `write`).
    for (size_t offset = range_member.offset; offset < range_member.end(); offset += block_size)
    {
        /// Tail block clamped to the file's real byte length.
        size_t this_block_size = std::min(block_size, file_size_in_bytes - offset);
        ByteRange block_range{offset, this_block_size};

        if (committed_ranges.subtract(block_range).empty())
            continue;

        if (!data.covers(block_range))
            continue;

        PageCacheByteRange byte_range{block_range.offset, block_range.size};
        UInt128 key_hash = byte_range.hash(base_hash);

        /// First-writer-wins: if another thread cached this block concurrently,
        /// `getOrSet` returns the existing cell and skips the load lambda.
        bool loaded = false;
        size_t loaded_bytes = 0;
        auto cell = cache->getOrSet(
            file,
            byte_range,
            /*detached_if_missing=*/false,
            inject_eviction,
            [&](const PageCache::MappedPtr & new_cell)
            {
                /// The cell expects block-relative layout: data must start at
                /// the block boundary and have no internal gaps. Partial-at-end
                /// (EOF) is fine.
                Rope slice = data.slice(block_range);
                ByteRange covered = slice.range();
                size_t pos = covered.size;
                if (pos > 0)
                {
                    if (covered.offset != block_range.offset)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "PageCacheWriter::write: data does not start at block boundary: "
                            "block=[{}, {}), covered=[{}, {})",
                            block_range.offset, block_range.end(), covered.offset, covered.end());

                    ByteRange to_copy{block_range.offset, pos};
                    if (!slice.covers(to_copy))
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "PageCacheWriter::write: data has internal gaps within block [{}, {})",
                            block_range.offset, block_range.end());

                    slice.copyTo(new_cell->data(), to_copy);
                }

                if (pos < new_cell->size())
                    std::memset(new_cell->data() + pos, 0, new_cell->size() - pos);

                loaded = true;
                loaded_bytes = pos;
            },
            key_hash);

        /// ALWAYS adopt the returned cell (loaded by us OR an existing
        /// first-writer cell) and mark the block committed - otherwise
        /// `complete` never becomes true under contention. Return only the
        /// bytes WE wrote.
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
                LOG_TRACE(log, "PageCacheWriter::write: populated block [{}, {})",
                    block_range.offset, block_range.end());
                bytes_written += loaded_bytes;
            }
        }
    }

    return bytes_written;
}

Rope PageCacheWriter::read(ByteRange sub)
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

    /// Serve the self-populated blocks overlapping `sub`, zero-copy.
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


PageCacheProvider::PageCacheProvider(
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

CacheViewPtr PageCacheProvider::planResidencyView(
    const StoredObject & /*object*/, size_t /*object_file_offset*/, ByteRange range_in_file)
{
    return buildView(range_in_file);
}

VectorWithMemoryTracking<MissEntry> PageCacheProvider::openWriteBuffers(
    const StoredObject & /*object*/,
    size_t /*object_file_offset*/,
    const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges)
{
    if (!populatesOnMiss())
        return {};

    VectorWithMemoryTracking<MissEntry> result;
    result.reserve(aligned_miss_ranges.size());

    /// PageCache is file-level - `object` / `object_file_offset` are ignored.
    /// Cells are created lazily on the first `write` of each block.
    for (const auto & aligned_file : aligned_miss_ranges)
    {
        auto writer = std::make_unique<PageCacheWriter>(
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

CacheViewPtr PageCacheProvider::buildView(ByteRange range_in_file)
{
    auto view = std::make_unique<PageCacheView>();

    /// Block-align the request; the tail block is clamped to the file's real
    /// byte length so a hit cell carries only valid bytes.
    size_t aligned_start = (range_in_file.offset / block_size) * block_size;
    size_t aligned_end = ((range_in_file.end() + block_size - 1) / block_size) * block_size;
    if (aligned_end > file_size_in_bytes)
        aligned_end = ((file_size_in_bytes + block_size - 1) / block_size) * block_size;

    SipHash base_hash = file.baseHash();

    /// Coalesce runs of adjacent same-kind blocks: a hit run collects its
    /// cells; a miss run only tracks the spanned range.
    VectorWithMemoryTracking<PageCacheReader::HeldCell> run_cells;
    ByteRange run_range{0, 0};
    bool run_active = false;
    bool run_is_hit = false;

    auto flush_run = [&]()
    {
        if (!run_active)
            return;
        if (run_is_hit)
        {
            auto reader = std::make_unique<PageCacheReader>(run_range, std::move(run_cells));
            view->hit_entries.push_back(HitEntry{run_range, std::move(reader)});
        }
        else
        {
            /// Writers are `openWriteBuffers`' job; views carry null.
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

        /// Read-only probe - `cache->get` never creates a cell.
        auto cell = cache->get(key_hash, inject_eviction);
        const bool is_hit = (cell != nullptr);

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
            run_cells.push_back(PageCacheReader::HeldCell{byte_range, std::move(cell)});
    }
    flush_run();

    LOG_TRACE(log, "PageCacheProvider::buildView: file [{}, {}) → {} hits, {} misses",
        range_in_file.offset, range_in_file.end(), view->hit_entries.size(), view->miss_entries.size());

    return view;
}

}
