#include <IO/PageCacheProvider.h>

#include <Common/ProfileEvents.h>
#include <base/defines.h>
#include <algorithm>
#include <cstring>

namespace ProfileEvents
{
    extern const Event PageCacheReadBytes;
}

namespace DB
{


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
    ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, hi - lo);
    return result;
}


PageCacheWriter::PageCacheWriter(
    PageCachePtr cache_,
    PageCacheFile file_,
    bool inject_eviction_,
    bool bypass_if_missing_,
    ByteRange block_range)
    : cache(std::move(cache_))
    , file(std::move(file_))
    , inject_eviction(inject_eviction_)
    , bypass_if_missing(bypass_if_missing_)
    , range_member(block_range)
{
}

size_t PageCacheWriter::write(ChainedBuffers data, [[maybe_unused]] const FillRole & role)
{
    /// A bypass tier populates nothing - skip before any `getOrSet`.
    if (bypass_if_missing)
        return 0;

    {
        std::lock_guard lock(committed_mutex);
        if (cell)  /// already committed (by us or an adopted first-writer cell)
            return 0;
    }

    /// Whole-block, all-or-nothing: a partially-covered block is left for a later `write`.
    if (!data.covers(range_member))
        return 0;

    PageCacheByteRange byte_range{range_member.offset, range_member.size};
    UInt128 key_hash = byte_range.hash(file.baseHash());

    /// First-writer-wins: if another thread cached this block concurrently, `getOrSet` returns the
    /// existing cell and skips the load lambda.
    bool loaded = false;
    auto got = cache->getOrSet(
        file,
        byte_range,
        /*detached_if_missing=*/false,
        inject_eviction,
        [&](const PageCache::MappedPtr & new_cell)
        {
            /// `data` covers the whole block (checked above); copy it in, zero any cell padding past it.
            data.slice(range_member).copyTo(new_cell->data(), range_member);
            if (range_member.size < new_cell->size())
                std::memset(new_cell->data() + range_member.size, 0, new_cell->size() - range_member.size);
            loaded = true;
        },
        key_hash);

    /// Adopt the returned cell (ours or the first-writer's) so `read`/`committed` see it. Return the
    /// bytes we wrote - 0 if we lost the first-writer race.
    if (got)
    {
        std::lock_guard lock(committed_mutex);
        cell = std::move(got);
    }
    return loaded ? range_member.size : 0;
}

CacheWriter::FillRole PageCacheWriter::takeFillRole()
{
    /// This tier elects no downloader, so the role only answers "is there anything left to fill".
    /// Re-probe read-only: if the block was cached by a concurrent query since `resolve`, adopt its cell
    /// (`committed()` then reports the whole block) and hold nothing; otherwise take the role to fill it.
    /// Two readers can both take it and both fetch; `getOrSet` keeps the first write.
    bool resident = false;
    if (auto got = cache->get(PageCacheByteRange{range_member.offset, range_member.size}.hash(file.baseHash()), inject_eviction))
    {
        std::lock_guard lock(committed_mutex);
        cell = std::move(got);
        resident = true;
    }

    return makeFillRole(/*held=*/!resident, /*release=*/nullptr);
}

ChainedBuffers PageCacheWriter::read(ByteRange sub)
{
    std::lock_guard lock(committed_mutex);
    ChainedBuffers result;
    if (!cell)
        return result;

    /// Clamp to this block's range, then emit one zero-copy node into the cell.
    const size_t lo = std::max(sub.offset, range_member.offset);
    const size_t hi = std::min(sub.end(), range_member.end());
    if (lo >= hi)
        return result;

    auto buf = std::make_shared<PageCacheChainedBuffer>(cell);
    result.append(ChainedBufferNode{std::move(buf), lo - range_member.offset, hi - lo, lo});
    ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, hi - lo);
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
    chassert(block_size > 0);  /// `resolve` divides by it
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
    const size_t file_size = file_size_in_bytes;
    if (range.offset >= file_size)
        return out;

    SipHash base_hash = file.baseHash();
    const size_t end_in_file = std::min(range.end(), file_size);
    const size_t first_pos = range.offset / block_size * block_size;
    out.reserve((end_in_file - first_pos + block_size - 1) / block_size);
    for (size_t pos = first_pos; pos < end_in_file; pos += std::min(block_size, file_size - pos))
    {
        const ByteRange block{pos, std::min(block_size, file_size - pos)};
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
            if (!bypass_if_missing)
                r.writer = std::make_unique<PageCacheWriter>(
                    cache, file, inject_eviction, bypass_if_missing, block);
        }
        out.push_back(std::move(r));
    }
    return out;
}

}
