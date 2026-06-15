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
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorWorkMicroseconds;
    extern const Event ReaderExecutorModeledCostMicroseconds;
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

void ReaderExecutor::Stats::add(Counter c, UInt64 value)
{
    values[c] += value;
    /// Each counter emits its ProfileEvent; cost-model counters also emit the modeled-cost
    /// contribution (weights documented at ProfileEvents::ReaderExecutorModeledCostMicroseconds).
    switch (c)
    {
        case SourceRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 30000 * value);
            break;
        case BytesFromSource:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorBytesFromSource, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 20000ULL * value / (1024 * 1024));
            break;
        case RequestedBytes:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorRequestedBytes, value);
            break;
        case IncompleteConnections:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorIncompleteConnections, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 5000 * value);
            break;
        case CacheGetRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 50 * value);
            break;
        case CachePopulateRequests:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateRequests, value);
            ProfileEvents::increment(ProfileEvents::ReaderExecutorModeledCostMicroseconds, 100 * value);
            break;
        case WorkMicroseconds:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorWorkMicroseconds, value);
            break;
        case NumCounters:
            break;
    }
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
    /// ProfileEvents are emitted instantly in `Stats::add`; `Stats` are read back here only
    /// for this summary report (a future PR turns it into a `system.reader_executor_log` row).
    LOG_DEBUG(log,
        "Destroyed: file={} src_reqs={} from_source={} requested={} work_us={}",
        log_file_path, stats.get(Stats::SourceRequests), stats.get(Stats::BytesFromSource),
        stats.get(Stats::RequestedBytes), stats.get(Stats::WorkMicroseconds));
}

ReaderExecutor::Chunk ReaderExecutor::readNextChunk()
{
    StatTimer work_timer(stats, Stats::WorkMicroseconds);

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

    /// One open+read per chunk; requested bytes equal source bytes until caches/over-read land.
    stats.add(Stats::SourceRequests);
    stats.add(Stats::BytesFromSource, got);
    stats.add(Stats::RequestedBytes, got);

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
