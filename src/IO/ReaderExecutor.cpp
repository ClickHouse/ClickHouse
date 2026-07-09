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
    extern const Event ReaderExecutorLongConnectionOpened;
    extern const Event ReaderExecutorLongConnectionHits;
    extern const Event ReaderExecutorLongConnectionFallbacks;
    extern const Event ReaderExecutorLongConnectionBytes;
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

/// Read `chunk` bytes from `buf` straight into `dest` with no intermediate copy when the buffer
/// honors an external buffer (set()+next() read directly into `dest`); short positive `next`
/// returns are looped so a partial fill never surfaces. Buffers that own a fixed-size internal
/// buffer (async, mmap, O_DIRECT) cannot accept an arbitrary `dest`, so fall back to read().
/// Returns the bytes read; less than `chunk` only at EOF.
static size_t readIntoBlock(ReadBuffer & buf, char * dest, size_t chunk)
{
    if (!buf.supportsExternalBufferMode())
        return buf.read(dest, chunk);

    size_t total = 0;
    while (total < chunk)
    {
        buf.set(dest + total, chunk - total);
        if (!buf.next())
            break;
        const size_t got = buf.available();
        if (got == 0)
            break;
        buf.position() = buf.buffer().end();
        total += got;
    }
    return total;
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
        case LongConnectionOpened:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionOpened, value);
            break;
        case LongConnectionHits:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionHits, value);
            break;
        case LongConnectionFallbacks:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionFallbacks, value);
            break;
        case LongConnectionBytes:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorLongConnectionBytes, value);
            break;
        case NumCounters:
            break;
    }
}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<IFileBasedSourceReader> source_,
    const StoredObjects & objects,
    Options options)
    : source(std::move(source_))
    , block_size(options.block_size ? options.block_size : DEFAULT_BLOCK_SIZE)
    , continuity_tracker(ReadContinuityTracker::Options{.bridgeable_gap = options.min_bytes_for_seek})
    , long_connection_limit(std::move(options.long_connection_limit))
    , min_bytes_for_seek(options.min_bytes_for_seek)
    , max_tail_for_drain(options.max_tail_for_drain)
    , active_metric(CurrentMetrics::ReaderExecutorActive)
{
    offset_map.build(objects);
    log_file_path = objects.empty() ? "" : objects.front().remote_path;
    LOG_DEBUG(log, "Created: source={}, objects={}, total_size={}, block_size={}, long_connections={}",
        source ? source->name() : "none", objects.size(), offset_map.totalSize(), block_size,
        long_connection_limit != nullptr);
}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<IFileBasedSourceReader> source_,
    const StoredObjects & objects)
    : ReaderExecutor(std::move(source_), objects, Options{})
{
}

ReaderExecutor::~ReaderExecutor()
{
    /// Release any held connection (drains a small tail to complete it, frees its slot, and
    /// accounts an incomplete drop if it was abandoned mid-response). `dropLongConnection`'s drain is
    /// best-effort and non-throwing, but keep a guard so nothing escapes a destructor -- a throw
    /// would `std::terminate`; the slot still releases via `long_conn`'s own destruction.
    try
    {
        dropLongConnection();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release a held source connection on destruction");
    }

    /// ProfileEvents are emitted instantly in `Stats::add`; `Stats` are read back here only
    /// for this summary report (a future PR turns it into a `system.reader_executor_log` row).
    LOG_DEBUG(log,
        "Destroyed: file={} src_reqs={} from_source={} requested={} work_us={}",
        log_file_path, stats.get(Stats::SourceRequests), stats.get(Stats::BytesFromSource),
        stats.get(Stats::RequestedBytes), stats.get(Stats::WorkMicroseconds));
}

size_t ReaderExecutor::LongConnection::readInto(char * dst, size_t want)
{
    if (want == 0)
        return 0;
    const size_t got = readIntoBlock(*buffer, dst, want);
    current_position += got;
    return got;
}

size_t ReaderExecutor::LongConnection::skipForward(size_t gap, size_t block_bytes)
{
    /// Discard through a scratch block: the bytes cross the wire (over-read) but the source
    /// request is saved. Short only at EOF.
    if (gap == 0)
        return 0;
    const size_t scratch_size = std::min(gap, block_bytes);
    auto scratch = std::make_shared<OwnedChainedBuffer>(scratch_size);
    size_t skipped = 0;
    while (skipped < gap)
    {
        const size_t got = readIntoBlock(*buffer, scratch->data(), std::min(gap - skipped, scratch_size));
        if (got == 0)
            break;
        skipped += got;
    }
    current_position += skipped;
    return skipped;
}

ReaderExecutor::LongConnection::DrainResult
ReaderExecutor::LongConnection::drainTail(size_t max_tail, size_t block_bytes, LoggerPtr logger) noexcept
{
    if (current_position >= read_until)
        return {};
    const size_t tail = read_until - current_position;
    if (tail > max_tail)
        return {};
    /// The drained tail is discarded -- it only lets the underlying HTTP connection return to the
    /// keep-alive pool -- so a read error here must not abort an otherwise valid query. Swallow it
    /// and report the failure; the caller then releases the connection as incomplete.
    try
    {
        return {.bytes = skipForward(tail, block_bytes), .failed = false};
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to drain a held source connection; releasing it as incomplete");
        return {.bytes = 0, .failed = true};
    }
}

size_t ReaderExecutor::clampReach(size_t reach, size_t logical_pos) const
{
    /// The estimator's reach is unclamped; bound it to the file end when the size is known.
    size_t end = logical_pos + reach;
    if (!offset_map.hasUnknownSize())
        end = std::min(end, totalSize());
    return end;
}

bool ReaderExecutor::shouldOpenLongConnection() const
{
    if (long_conn || !long_connection_limit)
        return false;
    /// Open a long connection when the predicted contiguous reach runs past this window.
    return clampReach(continuity_tracker.predictedForwardLength(), position) > position + block_size;
}

bool ReaderExecutor::tryOpenLongConnection(const StoredObject & object, size_t object_offset)
{
    auto slot = long_connection_limit->tryAcquire(long_connection_limit);
    if (!slot)
    {
        stats.add(Stats::LongConnectionFallbacks);
        return false;
    }

    /// Bound the held GET to the predicted forward reach (`clampReach` of the estimator), kept
    /// object-local and clamped to the object end. The estimate adapts: a long contiguous run
    /// grows it toward the whole object, while sparse access keeps it small -- so the connection
    /// reads ahead only as far as the pattern predicts (no full-object over-read when just a
    /// slice is used). Reuse spans this bound; once the read runs past it the connection completes
    /// (pool-reusable) and the next window opens a fresh, longer one as the run keeps growing.
    const size_t forward = clampReach(continuity_tracker.predictedForwardLength(), position) - position;
    size_t read_until_obj = object_offset + forward;
    if (!offset_map.hasUnknownSize())
        read_until_obj = std::min<size_t>(read_until_obj, object.bytes_size);

    auto buffer = source->open(object);
    if (buffer->supportsRightBoundedReads())
        buffer->setReadUntilPosition(read_until_obj);
    if (object_offset > 0)
        buffer->seek(static_cast<off_t>(object_offset), SEEK_SET);

    long_conn.emplace(LongConnection{
        .buffer = std::move(buffer),
        .object_path = object.remote_path,
        .opened_at = object_offset,
        .current_position = object_offset,
        .read_until = read_until_obj,
        .slot = std::move(slot),
    });
    stats.add(Stats::SourceRequests);
    stats.add(Stats::LongConnectionOpened);
    return true;
}

size_t ReaderExecutor::serveFromLongConnection(size_t object_offset, size_t want, char * dst)
{
    if (object_offset > long_conn->current_position)
    {
        /// Bridge a small forward gap by discarding it on the open stream.
        const size_t skipped = long_conn->skipForward(object_offset - long_conn->current_position, block_size);
        stats.add(Stats::BytesFromSource, skipped);
        stats.add(Stats::LongConnectionBytes, skipped);
    }
    const size_t got = long_conn->readInto(dst, want);
    stats.add(Stats::LongConnectionBytes, got);
    if (long_conn->atBound())
        long_conn.reset();   /// fully read to its bound -> pool-reusable, release the slot
    return got;
}

size_t ReaderExecutor::readOneShot(const StoredObject & object, size_t object_offset, size_t want, char * dst)
{
    auto buffer = source->open(object);
    /// Bound the request to the window (set before the seek so it applies to the GET).
    if (buffer->supportsRightBoundedReads())
        buffer->setReadUntilPosition(object_offset + want);
    if (object_offset > 0)
        buffer->seek(static_cast<off_t>(object_offset), SEEK_SET);
    stats.add(Stats::SourceRequests);
    return readIntoBlock(*buffer, dst, want);
}

void ReaderExecutor::dropLongConnection()
{
    if (!long_conn)
        return;
    /// Drain a small remaining tail so the connection completes and returns to the pool. The drain
    /// is best-effort (`drainTail` never throws): it reports whether it stopped short of its bound
    /// (EOF) and whether a read error interrupted it.
    bool ended_at_eof = false;
    bool drain_failed = false;
    if (!long_conn->atBound())
    {
        const auto drain = long_conn->drainTail(max_tail_for_drain, block_size, log);
        if (drain.bytes)
        {
            stats.add(Stats::BytesFromSource, drain.bytes);
            stats.add(Stats::LongConnectionBytes, drain.bytes);
        }
        drain_failed = drain.failed;
        ended_at_eof = !drain_failed && drain.bytes > 0 && !long_conn->atBound();
    }
    /// A connection abandoned mid-response (transferred, not complete) is not pool-reusable;
    /// one that never transferred (its lazy GET never issued) is excluded. A failed drain leaves the
    /// connection in an unknown state, so it is always incomplete.
    if (drain_failed || (long_conn->consumedAnyBytes() && !long_conn->isComplete(ended_at_eof)))
        stats.add(Stats::IncompleteConnections);
    long_conn.reset();
}

ChainedBuffers ReaderExecutor::readNextWindow()
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

    /// Clamp the block to the object boundary so a window never straddles two
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

    auto block = std::make_shared<OwnedChainedBuffer>(want);

    size_t got = 0;
    if (long_conn && long_conn->servesObject(object->remote_path)
        && long_conn->canContinue(object_offset, want, min_bytes_for_seek))
    {
        /// Reuse the held connection for this contiguous (or small-gap) window.
        stats.add(Stats::LongConnectionHits);
        got = serveFromLongConnection(object_offset, want, block->data());
    }
    else
    {
        /// A held connection that cannot continue (wrong object / backward / far / past
        /// bound) is dropped; then open a fresh long connection when the read is predicted
        /// to continue, else fall back to a one-shot read.
        if (long_conn)
            dropLongConnection();
        if (shouldOpenLongConnection() && tryOpenLongConnection(*object, object_offset))
            got = serveFromLongConnection(object_offset, want, block->data());
        else
            got = readOneShot(*object, object_offset, want, block->data());
    }

    /// Requested bytes equal source bytes until caches / over-read coalescing land; any bridged
    /// (skipped) bytes were already counted in BytesFromSource inside serveFromLongConnection.
    stats.add(Stats::BytesFromSource, got);
    stats.add(Stats::RequestedBytes, got);

    if (offset_map.hasUnknownSize())
    {
        /// At unknown total size a short read is the only EOF signal.
        if (got == 0)
        {
            reached_eof = true;
            long_conn.reset();   /// at EOF the connection is done, not an incomplete drop
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

    continuity_tracker.recordReadRange(position, got);

    ChainedBuffers chain;
    chain.append(ChainedBufferNode{std::move(block), 0, got, position});
    position += got;
    return chain;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_TRACE(log, "seek: {} -> {}", position, new_position);
    /// Feed the estimator; a held connection that can't continue to `new_position` is dropped
    /// lazily by the next `readNextWindow` (its `canContinue` check).
    continuity_tracker.recordSeek(new_position);
    position = new_position;
    reached_eof = false;
}

}
