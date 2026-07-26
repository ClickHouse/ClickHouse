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


PageCacheChainedBuffer::PageCacheChainedBuffer(PageCache::MappedPtr cell_)
    : cell(std::move(cell_))
{
}


PageCacheReader::PageCacheReader(ByteRange range_in_file, VectorWithMemoryTracking<HeldCell> cells_)
    : range_member(range_in_file)
    , cells(std::move(cells_))
{
}

ChainedBuffers PageCacheReader::read(ByteRange sub)
{
    ChainedBuffers result;

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

        auto buf = std::make_shared<PageCacheChainedBuffer>(held.cell);
        result.append(ChainedBufferNode{std::move(buf), offset_in_cell, overlap_size, overlap_start});
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

size_t PageCacheWriter::write(ChainedBuffers data)
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

        {
            std::lock_guard lock(state_mutex);
            if (committed_ranges.subtract(block_range).empty())
                continue;
        }

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
                ChainedBuffers slice = data.slice(block_range);
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
            {
                std::lock_guard lock(state_mutex);
                blocks.push_back(std::move(adopted));
                committed_ranges.add(block_range);
            }

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

ChainedBuffers PageCacheWriter::read(ByteRange sub)
{
    ChainedBuffers result;

    /// Clamp to this buffer's range (its adopted cells only back `range_member`).
    {
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;
        sub = ByteRange{lo, hi - lo};
    }

    /// Serve the self-populated blocks overlapping `sub`, zero-copy. Under the lock:
    /// a concurrent `write` on the same writer appends to `blocks`.
    std::lock_guard lock(state_mutex);
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

        auto buf = std::make_shared<PageCacheChainedBuffer>(block.cell);
        result.append(ChainedBufferNode{std::move(buf), offset_in_cell, overlap_size, overlap_start});
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

CacheWriterPtr PageCacheProvider::openWriter(
    const StoredObject & /*object*/,
    size_t /*object_file_offset*/,
    ByteRange cell)
{
    if (!populatesOnMiss())
        return nullptr;

    /// PageCache is file-level - `object` / `object_file_offset` are ignored.
    /// Cells are created lazily on the first `write` of each block.
    return std::make_unique<PageCacheWriter>(
        cache,
        file,
        block_size,
        file_size_in_bytes,
        inject_eviction,
        bypass_if_missing,
        cell);
}

/// The page tier's residency walk: stateless per step (block probes are cheap
/// map lookups), so the cursor only carries the provider reference.
class PageCacheProvider::ProbeCursor : public ICacheProvider::IProbeCursor
{
public:
    explicit ProbeCursor(PageCacheProvider & provider_) : provider(provider_) {}

    ICacheProvider::Resolution lookAt(
        const StoredObject & object, size_t object_file_offset, size_t pos_in_file) override
    {
        return provider.resolve(object, object_file_offset, pos_in_file);
    }

private:
    PageCacheProvider & provider;
};

std::unique_ptr<ICacheProvider::IProbeCursor> PageCacheProvider::probe()
{
    return std::make_unique<ProbeCursor>(*this);
}

ICacheProvider::Resolution PageCacheProvider::resolve(
    const StoredObject & /*object*/, size_t /*object_file_offset*/, size_t pos_in_file)
{
    /// PageCache is file-level - the object arguments are ignored. Blocks are
    /// probed read-only (`cache->get` never creates a cell); a hit run walks
    /// the contiguous mapped blocks from the asked one - capped so a fully
    /// warm file does not pin unboundedly through a single reader - and one
    /// reader holds the run's cells. A miss is ONE block cell.
    if (pos_in_file >= file_size_in_bytes)
        return {};

    static constexpr size_t HIT_RUN_CAP = 8 * 1024 * 1024;

    SipHash base_hash = file.baseHash();
    auto probe = [&](size_t off) -> PageCache::MappedPtr
    {
        const size_t sz = std::min(block_size, file_size_in_bytes - off);
        PageCacheByteRange byte_range{off, sz};
        return cache->get(byte_range.hash(base_hash), inject_eviction);
    };

    const size_t block_start = pos_in_file / block_size * block_size;
    Resolution res;
    auto first = probe(block_start);
    if (!first)
    {
        res.kind = Resolution::Kind::Miss;
        res.range = ByteRange{block_start, std::min(block_size, file_size_in_bytes - block_start)};
        return res;
    }

    VectorWithMemoryTracking<PageCacheReader::HeldCell> cells;
    size_t end = block_start;
    PageCache::MappedPtr cell = std::move(first);
    while (true)
    {
        const size_t sz = std::min(block_size, file_size_in_bytes - end);
        cells.push_back(PageCacheReader::HeldCell{PageCacheByteRange{end, sz}, std::move(cell)});
        end += sz;
        if (end >= file_size_in_bytes || end - block_start >= HIT_RUN_CAP)
            break;
        cell = probe(end);
        if (!cell)
            break;
    }
    res.kind = Resolution::Kind::Hit;
    res.range = ByteRange{block_start, end - block_start};
    res.reader = std::make_unique<PageCacheReader>(res.range, std::move(cells));
    return res;
}

}
