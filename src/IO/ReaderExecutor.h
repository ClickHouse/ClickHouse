#pragma once

#include <IO/Rope.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/ISourceReader.h>

#include <Common/Logger.h>
#include <future>
#include <memory>
#include <vector>

namespace DB
{

class PrefetchThreadPool;

class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_WINDOW_SIZE = 8 * 1024 * 1024; /// 8 MiB
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 8 * 1024 * 1024; /// 8 MiB

    ReaderExecutor(
        std::shared_ptr<ISourceReader> source,
        const StoredObjects & objects,
        std::vector<std::shared_ptr<ICacheProvider>> caches,
        size_t window_size = DEFAULT_WINDOW_SIZE,
        size_t min_bytes_for_seek = DEFAULT_MIN_BYTES_FOR_SEEK);

    /// Read the next window starting at the current position.
    /// Returns an empty Rope at EOF.
    Rope readNextWindow();

    /// Seek to a new position. Discards any prefetched data.
    void seek(size_t new_position);

    void setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool);

    size_t getPosition() const { return position; }
    size_t totalSize() const { return offset_map.totalSize(); }

    /// Merge close-together ranges to reduce source request count.
    /// Ranges separated by less than min_gap are combined.
    static std::vector<Range> mergeRanges(const std::vector<Range> & ranges, size_t min_gap);

private:
    /// Read a specific range through the cache chain and source.
    Rope readWindow(Range window);

    void maybeTriggerPrefetch();
    void discardPrefetch();

    std::shared_ptr<ISourceReader> source;
    OffsetMap offset_map;
    std::vector<std::shared_ptr<ICacheProvider>> caches;
    size_t window_size;
    size_t min_bytes_for_seek;
    size_t position = 0;

    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    std::future<Rope> prefetch_future;
    Range prefetch_range;      /// range the in-flight prefetch covers
    bool prefetch_valid = false;

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
