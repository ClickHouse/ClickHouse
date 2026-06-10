#include <IO/ReaderExecutor.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorWorkMicroseconds;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    /// ReaderExecutorCacheGetRequests / ReaderExecutorCachePopulateRequests /
    /// ReaderExecutorIncompleteConnections are declared in ProfileEvents.cpp and read 0
    /// until the cache and connection-reuse features add their increment sites.
}

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

namespace
{
    /// Heuristic S3 cost weights (microseconds) for the modeled-cost KPI, documented at
    /// `ProfileEvents::ReaderExecutorModeledCostMicroseconds`. Each counter contributes
    /// `weight * count` to the modeled cost as it changes. Only the source-side weights
    /// are used in this slice; the cache / connection weights land with their features.
    constexpr UInt64 SOURCE_REQUEST_COST_US = 30000;
    constexpr UInt64 SOURCE_BYTES_COST_US_PER_MIB = 20000;

    /// RAII timer: on destruction adds its lifetime (microseconds) to `counter` and
    /// increments the matching ProfileEvent, so a scope's elapsed time is accounted
    /// without a separate SCOPE_EXIT.
    class StatTimer
    {
    public:
        StatTimer(UInt64 & counter_, ProfileEvents::Event event_) : counter(counter_), event(event_) {}
        ~StatTimer()
        {
            const UInt64 us = watch.elapsedMicroseconds();
            counter += us;
            ProfileEvents::increment(event, us);
        }

        StatTimer(const StatTimer &) = delete;
        StatTimer & operator=(const StatTimer &) = delete;

    private:
        UInt64 & counter;
        ProfileEvents::Event event;
        Stopwatch watch;
    };
}

void ReaderExecutor::Stats::onSourceRead(size_t bytes)
{
    const UInt64 cost = SOURCE_REQUEST_COST_US + SOURCE_BYTES_COST_US_PER_MIB * bytes / (1024 * 1024);

    source_requests += 1;
    bytes_from_source += bytes;
    bytes_requested += bytes;
    modeled_cost_us += cost;

    ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceRequests, 1);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromSource, bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorRequestedBytes, bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, cost);
}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<IFileBasedSourceReader> source_,
    const StoredObjects & objects,
    size_t block_size_)
    : source(std::move(source_))
    , block_size(block_size_ ? block_size_ : DEFAULT_BLOCK_SIZE)
    , active_metric(CurrentMetrics::ReaderExecutorActive)
{
    offset_map.build(objects);
    log_file_path = objects.empty() ? "" : objects.front().remote_path;
    LOG_DEBUG(log, "Created: source={}, objects={}, total_size={}, block_size={}",
        source ? source->name() : "none", objects.size(), offset_map.totalSize(), block_size);
}

ReaderExecutor::~ReaderExecutor()
{
    /// ProfileEvents are incremented instantly in `readNextChunk` so `system.events`
    /// and the KPI reflect the executor in real time. The per-instance `Stats` mirror
    /// them only to write this final summary report (a future PR will also turn it
    /// into a `system.reader_executor_log` row).
    LOG_DEBUG(log,
        "Finished: file={}, source_requests={}, bytes_from_source={}, bytes_requested={}, "
        "work_us={}, modeled_cost_us={}",
        log_file_path, stats.source_requests, stats.bytes_from_source, stats.bytes_requested,
        stats.work_microseconds, stats.modeled_cost_us);
}

ReaderExecutor::Chunk ReaderExecutor::readNextChunk()
{
    StatTimer work_timer(stats.work_microseconds, ProfileEvents::ReaderExecutorWorkMicroseconds);

    if (atEnd())
        return {};

    size_t object_logical_start_offset = 0;
    const StoredObject * object = offset_map.findObjectAt(position, &object_logical_start_offset);
    if (!object)
    {
        reached_eof = true;
        return {};
    }

    const size_t object_offset = position - object_logical_start_offset;

    /// Clamp the block to the object boundary so a chunk never straddles two
    /// objects; the next call continues in the next object. Unknown total size
    /// means stream a full block and let a short read mark EOF.
    size_t want = block_size;
    if (!offset_map.hasUnknownSize())
    {
        const size_t remaining_in_object = object->bytes_size - object_offset;
        want = std::min(block_size, remaining_in_object);
        if (want == 0)
        {
            reached_eof = true;
            return {};
        }
    }

    /// `atEnd` already returned at the bound, so `position < *read_until` here.
    chassert(!read_until || *read_until >= position);
    if (read_until && *read_until - position < want)
        want = *read_until - position;

    auto buffer = source->open(*object);

    /// Bound the request to the chunk so a remote source fetches exactly `want`
    /// bytes rather than an open-ended tail that is then cancelled. Set before
    /// the seek so the bound applies to the connection opened on the first read.
    if (buffer->supportsRightBoundedReads())
        buffer->setReadUntilPosition(object_offset + want);

    if (object_offset > 0)
        buffer->seek(static_cast<off_t>(object_offset), SEEK_SET);

    block.resize(want);
    const size_t got = buffer->read(block.data(), want);

    /// One open+read per chunk in this minimal slice; every chunk is served straight
    /// from the source. `onSourceRead` updates the tally and increments the matching
    /// ProfileEvents instantly.
    stats.onSourceRead(got);

    if (offset_map.hasUnknownSize())
    {
        /// At unknown total size a short read is the only EOF signal.
        if (got == 0)
        {
            reached_eof = true;
            return {};
        }
    }
    else if (got < want)
    {
        /// The object is shorter than its declared size — truncated or corrupt.
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "ReaderExecutor: read {} of {} bytes at offset {} from {} (declared size {})",
            got, want, position, object->remote_path, object->bytes_size);
    }

    Chunk chunk{block.data(), got, position};
    position += got;
    return chunk;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_TRACE(log, "seek: {} -> {}", position, new_position);
    position = new_position;
    reached_eof = false;
}

}
