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


PageCacheReader::PageCacheReader(ByteRange range_in_file, PageCache::MappedPtr cell_)
    : range_member(range_in_file)
    , cell(std::move(cell_))
{
}

ChainedBuffers PageCacheReader::read(ByteRange sub)
{
    ChainedBuffers result;
    if (!cell)
        return result;

    /// Clamp `sub` to this block's range (`range_member == cell->range`), then emit one zero-copy node
    /// into the cell at the matching offset.
    const size_t lo = std::max(sub.offset, range_member.offset);
    const size_t hi = std::min(sub.end(), range_member.end());
    if (lo >= hi)
        return result;

    auto buf = std::make_shared<PageCacheChainedBuffer>(cell);
    result.append(ChainedBufferNode{std::move(buf), lo - range_member.offset, hi - lo, lo});
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

size_t PageCacheWriter::write(ChainedBuffers data, [[maybe_unused]] const Claim & claim)
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
            {
                std::lock_guard lock(state_mutex);
                blocks.push_back(cell);
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

CacheWriter::Lead PageCacheWriter::claimLeadRole(ByteRange range)
{
    /// Re-probe read-only: adopt any prefix a concurrent query cached since `resolve`, report it as
    /// `available`, and return a held claim only when an uncommitted tail is left to fill.
    Lead lead;
    lead.available = ByteRange{range.offset, 0};

    SipHash base_hash = file.baseHash();
    std::lock_guard lock(state_mutex);
    for (size_t off = range.offset; off < range.end(); off += block_size)
    {
        const size_t sz = std::min(block_size, file_size_in_bytes - off);
        auto cell = cache->get(PageCacheByteRange{off, sz}.hash(base_hash), inject_eviction);
        if (!cell)
            break;
        blocks.push_back(std::move(cell));
        committed_ranges.add(ByteRange{off, sz});
        lead.available = ByteRange{range.offset, std::min(off + sz, range.end()) - range.offset};
    }

    lead.claim = makeClaim(/*held=*/lead.available.end() < range.end(), /*release=*/nullptr);
    return lead;
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

    /// Serve the self-populated blocks overlapping `sub`, zero-copy, under the lock that guards `blocks`.
    std::lock_guard lock(state_mutex);
    for (const auto & cell : blocks)
    {
        if (!cell)
            continue;

        ByteRange block_range{cell->range.offset, cell->range.size};
        if (block_range.end() <= sub.offset || block_range.offset >= sub.end())
            continue;

        size_t overlap_start = std::max(block_range.offset, sub.offset);
        size_t overlap_end = std::min(block_range.end(), sub.end());
        size_t offset_in_cell = overlap_start - block_range.offset;
        size_t overlap_size = overlap_end - overlap_start;

        auto buf = std::make_shared<PageCacheChainedBuffer>(cell);
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

/// The page tier's residency walk over `range`: one resolution per whole block. It holds no per-call
/// state, so a shared provider is safe to resolve concurrently. Each block is probed read-only
/// (`cache->get` never creates a cell): a cached block is a hit whose reader holds that cell, an
/// uncached block is a miss carrying its whole-block writer when the tier populates (writer-less on a
/// bypass tier). The executor gathers consecutive misses for the fetch and serves one block per window.
VectorWithMemoryTracking<ICacheProvider::CacheResolution> PageCacheProvider::resolve(
    const StoredObject & /*object*/, size_t /*object_file_offset*/, ByteRange range)
{
    VectorWithMemoryTracking<ICacheProvider::CacheResolution> out;
    const size_t blk = block_size;
    const size_t file_size = file_size_in_bytes;
    if (range.offset >= file_size)
        return out;

    SipHash base_hash = file.baseHash();
    const size_t end_in_file = std::min(range.end(), file_size);
    for (size_t pos = range.offset / blk * blk; pos < end_in_file; pos += std::min(blk, file_size - pos))
    {
        const ByteRange block{pos, std::min(blk, file_size - pos)};
        CacheResolution r;
        r.range = block;
        if (auto cell = cache->get(PageCacheByteRange{block.offset, block.size}.hash(base_hash), inject_eviction))
        {
            r.kind = CacheResolution::Kind::Hit;
            r.reader = std::make_unique<PageCacheReader>(block, std::move(cell));
        }
        else
        {
            r.kind = CacheResolution::Kind::Miss;
            if (populatesOnMiss())
                r.writer = std::make_unique<PageCacheWriter>(
                    cache, file, blk, file_size, inject_eviction, bypass_if_missing, block);
        }
        out.push_back(std::move(r));
    }
    return out;
}

}
