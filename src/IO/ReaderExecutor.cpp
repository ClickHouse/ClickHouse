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
    size_t window_size_,
    size_t min_bytes_for_seek_)
    : source(std::move(source_))
    , caches(std::move(caches_))
    , window_size(window_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
    offset_map.build(objects);
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, caches.size());
}

std::vector<Range> ReaderExecutor::mergeRanges(const std::vector<Range> & ranges, size_t min_gap)
{
    if (ranges.empty() || min_gap == 0)
        return ranges;

    /// Ranges should already be sorted by offset (they come from cache lookups
    /// that walk the window left to right), but sort just in case.
    std::vector<Range> sorted = ranges;
    std::sort(sorted.begin(), sorted.end(),
        [](const Range & a, const Range & b) { return a.offset < b.offset; });

    std::vector<Range> merged;
    merged.push_back(sorted[0]);

    for (size_t i = 1; i < sorted.size(); ++i)
    {
        auto & prev = merged.back();
        size_t gap = sorted[i].offset - prev.end();

        if (gap <= min_gap)
        {
            /// Merge: extend prev to cover sorted[i]
            size_t new_end = std::max(prev.end(), sorted[i].end());
            prev.size = new_end - prev.offset;
        }
        else
        {
            merged.push_back(sorted[i]);
        }
    }

    return merged;
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

    /// Merge close-together ranges to reduce source request count.
    /// E.g. scattered page cache hits can leave many small gaps that are
    /// cheaper to read in one request than to issue separate HTTP GETs.
    auto fetch_ranges = mergeRanges(remaining, min_bytes_for_seek);

    if (fetch_ranges.size() < remaining.size())
        LOG_TRACE(log, "readWindow: merged {} miss ranges into {} fetch ranges (min_gap={})",
            remaining.size(), fetch_ranges.size(), min_bytes_for_seek);

    /// Fetch from source
    for (const auto & miss_range : fetch_ranges)
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
