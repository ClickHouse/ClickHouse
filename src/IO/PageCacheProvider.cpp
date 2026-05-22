#include <IO/PageCacheProvider.h>

#include <Common/logger_useful.h>
#include <cstring>

namespace DB
{

PageCacheHandle::PageCacheHandle(
    PageCacheFile file_,
    ByteRange requested,
    PageCachePtr cache_,
    size_t block_size,
    bool inject_eviction_)
    : file(std::move(file_))
    , cache(std::move(cache_))
    , inject_eviction(inject_eviction_)
{
    /// Split the requested range into block-aligned chunks.
    size_t aligned_start = (requested.offset / block_size) * block_size;
    size_t aligned_end = ((requested.end() + block_size - 1) / block_size) * block_size;

    SipHash base_hash = file.baseHash();

    for (size_t offset = aligned_start; offset < aligned_end; offset += block_size)
    {
        PageCacheByteRange byte_range{offset, block_size};
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
        auto cell = cache->getOrSet(
            file,
            block.byte_range,
            /*detached_if_missing=*/false,
            inject_eviction,
            [&](const PageCache::MappedPtr & new_cell)
            {
                /// Copy data from the Rope into the cache cell.
                Rope slice = data.slice(block_range);

                size_t pos = 0;
                for (const auto & node : slice.getNodes())
                {
                    size_t copy_size = std::min(node.size, new_cell->size() - pos);
                    std::memcpy(new_cell->data() + pos, node.data(), copy_size);
                    pos += copy_size;
                }

                /// If data didn't fully cover the block (e.g. end of file),
                /// zero the remaining bytes.
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


std::unique_ptr<ICacheHandle> PageCacheProvider::lookup(CacheKey key, ByteRange range)
{
    PageCacheFile file;
    file.path = std::move(key.path);
    file.file_version = std::move(key.version);

    return std::make_unique<PageCacheHandle>(
        std::move(file), range, cache, block_size, inject_eviction);
}

}
