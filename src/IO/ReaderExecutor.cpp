#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>

#include <Common/logger_useful.h>
#include <algorithm>

namespace DB
{

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<ISourceReader> source_,
    const StoredObjects & objects,
    std::vector<std::shared_ptr<ICacheProvider>> caches_,
    size_t window_size_)
    : source(std::move(source_))
    , caches(std::move(caches_))
    , window_size(window_size_)
{
    offset_map.build(objects);
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, caches.size());
}

void ReaderExecutor::setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool)
{
    prefetch_pool = std::move(pool);
}

void ReaderExecutor::maybeTriggerPrefetch()
{
    if (!prefetch_pool || prefetch_valid)
        return;

    size_t total = offset_map.totalSize();
    if (position >= total)
        return;

    size_t next_size = std::min(window_size, total - position);
    Range next_window{position, next_size};

    LOG_TRACE(log, "Prefetch: submitting [{}, {})", next_window.offset, next_window.end());

    prefetch_future = prefetch_pool->submit([this, next_window]()
    {
        return readWindow(next_window);
    });
    prefetch_range = next_window;
    prefetch_valid = true;
}

void ReaderExecutor::discardPrefetch()
{
    if (prefetch_valid)
    {
        LOG_TRACE(log, "Prefetch: discarding [{}, {})", prefetch_range.offset, prefetch_range.end());
        if (prefetch_future.valid())
            std::ignore = prefetch_future.get();
        prefetch_valid = false;
    }
}

Rope ReaderExecutor::readNextWindow()
{
    size_t total = offset_map.totalSize();
    if (position >= total)
    {
        LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
        return {};
    }

    Rope rope;

    if (prefetch_valid && prefetch_future.valid())
    {
        LOG_TRACE(log, "readNextWindow: using prefetched [{}, {})", prefetch_range.offset, prefetch_range.end());
        rope = prefetch_future.get();
        prefetch_valid = false;
    }
    else
    {
        size_t win_size = std::min(window_size, total - position);
        Range window{position, win_size};
        LOG_TRACE(log, "readNextWindow: synchronous read [{}, {})", window.offset, window.end());
        rope = readWindow(window);
    }

    position += rope.range().size;
    LOG_TRACE(log, "readNextWindow: got {} bytes, {} nodes, position advanced to {}",
        rope.range().size, rope.getNodes().size(), position);

    maybeTriggerPrefetch();

    return rope;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_DEBUG(log, "seek to {}, current position={}", new_position, position);

    if (prefetch_valid
        && new_position >= prefetch_range.offset
        && new_position < prefetch_range.end())
    {
        LOG_TRACE(log, "seek: target within prefetch [{}, {}), keeping prefetch",
            prefetch_range.offset, prefetch_range.end());
        position = prefetch_range.offset;
        return;
    }

    discardPrefetch();
    position = new_position;
}

Rope ReaderExecutor::readWindow(Range window)
{
    LOG_TRACE(log, "readWindow [{}, {})", window.offset, window.end());

    Rope result;
    std::vector<Range> remaining = {window};

    /// Walk the cache chain
    for (auto & cache : caches)
    {
        std::vector<Range> still_missing;

        for (const auto & r : remaining)
        {
            auto handle = cache->lookup(CacheKey{}, r);
            auto status = handle->status();

            for (const auto & hit : status.hit_ranges)
            {
                LOG_TRACE(log, "readWindow: cache {} hit [{}, {})", cache->name(), hit.offset, hit.end());
                auto slice = handle->get(hit);
                for (const auto & node : slice.getNodes())
                    result.append(RopeNode{node.buffer, node.buffer_offset, node.size, node.logical_offset});
            }

            for (const auto & miss : status.miss_ranges)
            {
                LOG_TRACE(log, "readWindow: cache {} miss [{}, {})", cache->name(), miss.offset, miss.end());
                still_missing.push_back(miss);
            }
        }

        remaining = std::move(still_missing);
        if (remaining.empty())
            break;
    }

    /// Fetch remaining from source
    for (const auto & miss_range : remaining)
    {
        auto physical_ranges = offset_map.map(miss_range);
        size_t logical_pos = miss_range.offset;

        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "readWindow: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);
            auto buf = std::make_shared<OwnedRopeBuffer>(pr.size);
            size_t bytes_read = source->read(pr.object, pr.object_offset, pr.size, buf->data());

            result.append(RopeNode{std::move(buf), 0, bytes_read, logical_pos});
            logical_pos += bytes_read;
        }

        /// Fill caches bottom-up with fetched data
        for (auto it = caches.rbegin(); it != caches.rend(); ++it)
        {
            auto handle = (*it)->lookup(CacheKey{}, miss_range);
            auto status = handle->status();
            for (const auto & miss : status.miss_ranges)
            {
                auto slice = result.slice(miss);
                if (!slice.empty())
                    handle->put(miss, std::move(slice));
            }
        }
    }

    /// Sort nodes by logical offset
    auto & nodes = const_cast<std::vector<RopeNode> &>(result.getNodes());
    std::sort(nodes.begin(), nodes.end(),
        [](const RopeNode & a, const RopeNode & b)
        { return a.logical_offset < b.logical_offset; });

    /// Trim to the originally requested window.
    return result.slice(window);
}

}
