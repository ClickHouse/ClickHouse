#include <IO/ReaderExecutor.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Cache/EncryptionHeaderCache.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <cstring>

namespace ProfileEvents
{
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorDeliveredBytes;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorWorkMicroseconds;
    extern const Event ReaderExecutorDecryptMicroseconds;
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

/// Read `chunk` bytes into `dest` with no intermediate copy when the buffer honors an external
/// buffer; buffers with a fixed internal buffer (async, mmap, O_DIRECT) fall back to `read`. A
/// return below `chunk` means EOF.
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
    /// Each counter emits its ProfileEvent; cost-model counters also add the modeled-cost weight.
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
        case DeliveredBytes:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorDeliveredBytes, value);
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
        case DecryptMicroseconds:
            ProfileEvents::increment(ProfileEvents::ReaderExecutorDecryptMicroseconds, value);
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
    , window_size(options.window_size)
    , block_size(options.block_size)
    , fetch_tracker(ReadContinuityTracker::Options{.bridgeable_gap = options.min_bytes_for_seek})
    , long_connection_limit(std::move(options.long_connection_limit))
    , encryption_header_cache(std::move(options.encryption_header_cache))
    , cache_chain(std::move(options.cache_chain))
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
    /// `dropLongConnection` is best-effort and non-throwing, but guard anyway so nothing escapes
    /// the destructor.
    try
    {
        dropLongConnection();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release a held source connection on destruction");
    }

    LOG_DEBUG(log,
        "Destroyed: file={} src_reqs={} from_source={} requested={} work_us={}",
        log_file_path, stats.get(Stats::SourceRequests), stats.get(Stats::BytesFromSource),
        stats.get(Stats::DeliveredBytes), stats.get(Stats::WorkMicroseconds));
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
    /// Discard through a scratch block: the bytes cross the wire but a fresh source request is
    /// saved. Short only at EOF.
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
    /// The drained tail only returns the connection to the keep-alive pool, so a read error here
    /// must not abort the query: swallow it and report the failure via `DrainResult::failed`.
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

size_t ReaderExecutor::clampReach(size_t predicted_end, size_t phys_pos) const
{
    /// Bound the run-anchored predicted end (physical) to `[phys_pos, physical file end]`;
    /// with an unknown total size there is no end to clamp to.
    size_t end = std::max(predicted_end, phys_pos);
    if (!offset_map.hasUnknownSize())
        end = std::min(end, offset_map.totalSize());
    return end;
}

bool ReaderExecutor::shouldOpenLongConnection() const
{
    if (long_conn || !long_connection_limit)
        return false;
    /// Open a long connection when the predicted run end runs past this window (physical coords).
    const size_t phys = toPhysical(position);
    return clampReach(fetch_tracker.predictedEnd(), phys) > phys + window_size;
}

bool ReaderExecutor::tryOpenLongConnection(const StoredObject & object, size_t object_offset)
{
    auto slot = long_connection_limit->tryAcquire(long_connection_limit);
    if (!slot)
    {
        stats.add(Stats::LongConnectionFallbacks);
        return false;
    }

    /// Bound the held GET to the predicted run end, clamped to the object -- a growing run reads
    /// further ahead while sparse access stays small.
    const size_t phys = toPhysical(position);
    const size_t forward = clampReach(fetch_tracker.predictedEnd(), phys) - phys;
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

ChainedBuffers ReaderExecutor::readObjectSlice(const StoredObject & object, size_t object_offset, size_t want, size_t file_base)
{
    ChainedBuffers chain;
    size_t got_total = 0;
    auto fill = [&](size_t limit, auto && read_chunk)
    {
        while (got_total < limit)
        {
            const size_t chunk = std::min(block_size, limit - got_total);
            auto block = std::make_shared<OwnedChainedBuffer>(chunk);
            const size_t n = read_chunk(block->data(), chunk);
            if (n > 0)
            {
                chain.append(ChainedBufferNode{std::move(block), 0, n, file_base + got_total});
                got_total += n;
            }
            if (n < chunk)
                break;
        }
    };
    auto from_long_conn = [&](char * dst, size_t n)
    {
        const size_t read_bytes = long_conn->readInto(dst, n);
        stats.add(Stats::LongConnectionBytes, read_bytes);
        return read_bytes;
    };

    if (long_conn && long_conn->servesObject(object.remote_path)
        && long_conn->canServeAt(object_offset, min_bytes_for_seek))
    {
        stats.add(Stats::LongConnectionHits);
        if (object_offset > long_conn->current_position)
        {
            const size_t skipped = long_conn->skipForward(object_offset - long_conn->current_position, block_size);
            stats.add(Stats::BytesFromSource, skipped);
            stats.add(Stats::LongConnectionBytes, skipped);
        }
        /// Serve only up to the bound (short window); avoids draining and re-reading the tail.
        const size_t serve = std::min(want, long_conn->read_until - object_offset);
        fill(serve, from_long_conn);
        if (long_conn->atBound())
            long_conn.reset();
    }
    else
    {
        if (long_conn)
            dropLongConnection();
        if (shouldOpenLongConnection() && tryOpenLongConnection(object, object_offset))
        {
            fill(want, from_long_conn);
            if (long_conn && long_conn->atBound())
                long_conn.reset();
        }
        else
        {
            auto buffer = source->open(object);
            if (buffer->supportsRightBoundedReads())
                buffer->setReadUntilPosition(object_offset + want);
            if (object_offset > 0)
                buffer->seek(static_cast<off_t>(object_offset), SEEK_SET);
            stats.add(Stats::SourceRequests);
            fill(want, [&](char * dst, size_t n) { return readIntoBlock(*buffer, dst, n); });
        }
    }

    stats.add(Stats::BytesFromSource, got_total);
    fetch_tracker.recordReadRange(file_base, got_total);
    return chain;
}

ChainedBuffers ReaderExecutor::readSource(size_t file_offset, size_t want)
{
    ChainedBuffers chain;
    size_t file_pos = file_offset;
    for (const auto & object_range : offset_map.map(ByteRange{file_offset, want}))
    {
        ChainedBuffers piece = readObjectSlice(
            object_range.object, object_range.object_offset, object_range.size, file_pos);
        const size_t got = piece.range().size;
        chain.append(std::move(piece));
        file_pos += got;
        /// A short read leaves a hole; stop and let the caller advance and re-call.
        if (got < object_range.size)
            break;
    }
    return chain;
}

ChainedBuffers ReaderExecutor::readThroughCaches(size_t window_offset, size_t max_serve)
{
    chassert(!cache_chain.empty());
    /// Resolve the whole window across tiers (front = fastest) and act on the run covering the head:
    /// a hit serves one block from cache, an all-miss fetches the covering range(s) and populates.
    /// Coarse fetch, fine serve.
    const auto start_piece = offset_map.map(ByteRange{window_offset, 1});
    chassert(!start_piece.empty());
    const StoredObject & object = start_piece.front().object;
    const size_t object_offset = start_piece.front().object_offset;

    /// Serve one block from `window_offset`, capped by the window and by what is available up to `end`.
    auto serve_len = [&](size_t end) { return std::min({block_size, max_serve, end - window_offset}); };

    /// A miss to be populated carries its own open writer; any other miss is writer-less. `role` is
    /// filled by the role loop below (empty for a writer-less miss, or a tail another reader leads).
    struct MissTier { CacheWriterPtr writer; ByteRange range; CacheWriter::FillRole role; };
    VectorWithMemoryTracking<MissTier> miss_tiers;
    size_t next_resident = window_offset + max_serve;  /// nearest block any tier already has, ahead of the head
    for (auto & cache : cache_chain)
    {
        stats.add(Stats::CacheGetRequests);
        /// `resolve` returns the window's residency in offset order, coverage contiguous from the ask
        /// start. The run covering `window_offset` decides the tier: a hit there serves the block from
        /// cache. A miss is recorded, and we keep gathering the contiguous miss run behind it so the
        /// fetch below reads the whole uncached extent in one source request; a later hit ends the run.
        auto resolutions = cache->resolve(object, object_offset, ByteRange{window_offset, max_serve});
        for (auto & resolution : resolutions)
        {
            if (resolution.range.end() <= window_offset)
                continue;
            if (resolution.kind == ICacheProvider::CacheResolution::Kind::Hit)
            {
                if (resolution.range.offset <= window_offset && resolution.reader)
                    return resolution.reader->read(ByteRange{window_offset, serve_len(resolution.range.end())});
                next_resident = std::min(next_resident, resolution.range.offset);  /// a resident block ahead
                break;
            }
            miss_tiers.push_back(MissTier{std::move(resolution.writer), resolution.range, {}});
        }
    }

    /// Every tier missed: role each writing tier's lead role before the fetch (a held role dedups the
    /// download). If `takeFillRole` reports a prefix cached since `resolve` covering the head, serve it
    /// from that tier - no source read; otherwise keep the role for the fetch.
    bool any_writer = false;
    for (auto & miss_tier : miss_tiers)
    {
        if (!miss_tier.writer)
            continue;  /// nothing will be populated here
        any_writer = true;
        CacheWriter::FillRole role = miss_tier.writer->takeFillRole();
        /// A prefix cached since `resolve` (`committed()`) covering the head is served from that tier - no
        /// source read; otherwise keep the role for the fetch.
        const size_t avail_end = miss_tier.writer->committed();
        if (miss_tier.writer->range().offset <= window_offset && window_offset < avail_end)
            return miss_tier.writer->read(ByteRange{window_offset, serve_len(std::min(avail_end, miss_tier.range.end()))});
        miss_tier.role = std::move(role);
    }

    /// A range another thread is already downloading is fetched through below (its `write` lands 0).

    /// No writing tier (all bypass): the miss range is the exact uncached extent, so read it whole from
    /// source and serve all of it - nothing is cached inside it, and we store nothing, so there is no
    /// waste. Cap at the nearest tier boundary (`miss_end`) so the next window re-probes there and picks
    /// up a cell a tier has cached.
    if (!any_writer)
    {
        size_t miss_end = window_offset + max_serve;
        for (const auto & miss_tier : miss_tiers)
            miss_end = std::min(miss_end, miss_tier.range.end());
        return readSource(window_offset, miss_end - window_offset);
    }

    /// Fetch the writer ranges in one source read (across the objects they span) and populate each.
    /// Capped at `next_resident` - the nearest block a slower tier already holds - so a gathered miss run
    /// never re-reads a suffix the filesystem cache already has; that suffix is served from the slower
    /// tier on the next window.
    size_t fetch_lo = window_offset;
    size_t fetch_hi = window_offset;
    for (const auto & miss_tier : miss_tiers)
    {
        if (!miss_tier.writer)
            continue;
        fetch_lo = std::min(fetch_lo, miss_tier.range.offset);
        fetch_hi = std::max(fetch_hi, miss_tier.range.end());
    }
    fetch_hi = std::min<size_t>(fetch_hi, offset_map.totalSize());
    fetch_hi = std::min(fetch_hi, next_resident);  /// do not re-read a block a slower tier already has

    ChainedBuffers fetched = readSource(fetch_lo, fetch_hi - fetch_lo);
    const size_t fetched_end = fetched.empty() ? fetch_lo : fetched.range().end();

    for (auto & miss_tier : miss_tiers)
    {
        /// Only a held role authorizes a write; a tier led by a concurrent downloader is filled by
        /// that thread, not here.
        if (miss_tier.role)
        {
            const size_t lo = std::max(miss_tier.range.offset, fetch_lo);
            const size_t hi = std::min(miss_tier.range.end(), fetched_end);
            /// From the whole file-level fetch, so a block straddling objects is covered.
            if (lo < hi)
            {
                const ByteRange write_range{lo, hi - lo};
                if (fetched.covers(write_range))
                {
                    stats.add(Stats::CachePopulateRequests);
                    miss_tier.writer->write(fetched.slice(write_range), miss_tier.role);
                }
            }
        }
        /// Free the downloader role as soon as this tier is done, not at window end: reset the role
        /// (it completes+resets the role while we still hold it). The writer is finalized when
        /// `miss_tiers` is destroyed.
        miss_tier.role.reset();
    }

    return fetched.slice(ByteRange{window_offset, serve_len(fetched_end)});
}

void ReaderExecutor::dropLongConnection()
{
    if (!long_conn)
        return;
    /// Drain a small remaining tail so the connection completes and returns to the pool
    /// (best-effort; `drainTail` never throws).
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
    /// A connection abandoned mid-response is not pool-reusable; one that never transferred is
    /// excluded; a failed drain leaves it in an unknown state, so it counts as incomplete.
    if (drain_failed || (long_conn->consumedAnyBytes() && !long_conn->isComplete(ended_at_eof)))
        stats.add(Stats::IncompleteConnections);
    long_conn.reset();
}

size_t ReaderExecutor::totalSize() const
{
    const size_t physical = offset_map.totalSize();
    /// An empty source has no header; a non-empty one always has the full header, so `physical` is
    /// either 0 or >= data_start_offset.
    if (physical == 0)
        return 0;
    return toLogical(physical);
}

void ReaderExecutor::addDecryptionLayer(
    [[maybe_unused]] String path,
    [[maybe_unused]] KeyFinderFunc key_finder)
{
#if USE_SSL
    decryptor.addLayer(std::move(path), std::move(key_finder));
    data_start_offset = decryptor.headerBytes();
    LOG_DEBUG(log, "Added decryption layer, data_start_offset={}", data_start_offset);
#endif
}

void ReaderExecutor::initDecryption()
{
#if USE_SSL
    if (decryptor.initialized() || decryptor.empty())
        return;

    const size_t total_source_size = offset_map.totalSize();

    /// An empty underlying source has no encryption header; skip (subsequent reads return 0 bytes,
    /// like an empty file on an unencrypted disk).
    if (total_source_size == 0)
    {
        LOG_DEBUG(log, "initDecryption: source is empty, skipping");
        return;
    }

    /// Source exists but is smaller than the header(s) — corrupted.
    if (total_source_size < data_start_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Encrypted source has {} bytes, less than header size {}",
            total_source_size, data_start_offset);

    /// The headers sit at the front of the first object; identify it (its `remote_path` is the
    /// stable cache key for disk files).
    const auto head = offset_map.map(ByteRange{0, 1});
    if (head.empty())
        return;
    const StoredObject & object = head.front().object;

    /// Cache hit: parse the cached header bytes and skip the source read. The size check guards
    /// against a stale entry from a differently-layered file at the same path.
    if (encryption_header_cache)
    {
        if (auto cached = encryption_header_cache->read(object.remote_path);
            cached && cached->size() == data_start_offset)
        {
            auto cached_block = std::make_shared<OwnedChainedBuffer>(data_start_offset);
            std::memcpy(cached_block->data(), cached->data(), data_start_offset);
            ChainedBuffers header_chain;
            header_chain.append(ChainedBufferNode{std::move(cached_block), 0, data_start_offset, 0});
            decryptor.parseHeaders(header_chain);
            return;
        }
    }

    LOG_DEBUG(log, "initDecryption: reading headers ({} bytes)", data_start_offset);

    /// Miss: read the headers from the source (one-shot; no long connection).
    auto block = std::make_shared<OwnedChainedBuffer>(data_start_offset);
    const size_t got = readOneShot(object, /*object_offset=*/0, data_start_offset, block->data());

    /// Under a size-unknown source a short read means EOF rather than an error: 0 bytes is an empty
    /// object (as the size-known empty branch above), while a partial read is corruption.
    if (offset_map.hasUnknownSize() && got == 0)
    {
        LOG_DEBUG(log, "initDecryption: unknown-size source returned 0 bytes (empty object), skipping");
        return;
    }
    if (got != data_start_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Encrypted source returned {} header bytes, expected {} (corrupted/truncated)",
            got, data_start_offset);

    ChainedBuffers header_chain;
    header_chain.append(ChainedBufferNode{block, 0, got, 0});
    decryptor.parseHeaders(header_chain);

    if (encryption_header_cache)
        encryption_header_cache->write(object.remote_path, String(block->data(), got));
#endif
}

void ReaderExecutor::decryptInPlaceIfNeeded(
    [[maybe_unused]] char * data, [[maybe_unused]] size_t size, [[maybe_unused]] size_t logical_offset)
{
#if USE_SSL
    if (decryptor.empty() || size == 0)
        return;
    StatTimer decrypt_scope(stats, Stats::DecryptMicroseconds);
    decryptor.decrypt(data, size, logical_offset);
#endif
}

ChainedBuffers ReaderExecutor::readNextWindow()
{
    StatTimer work_timer(stats, Stats::WorkMicroseconds);

    if (atEnd())
        return {};

    const size_t position_physical = toPhysical(position);

    /// The most this window may serve: `window_size`, clamped to the file end (when the size is
    /// known) and to the `read_until` bound. The cache path serves at most `block_size` of it; the
    /// no-cache path serves it whole.
    size_t max_serve = window_size;
    if (!offset_map.hasUnknownSize())
        max_serve = std::min(max_serve, offset_map.totalSize() - position_physical);
    chassert(!read_until || *read_until >= position);
    if (read_until && *read_until - position < max_serve)
        max_serve = *read_until - position;
    if (max_serve == 0)
    {
        reached_eof = true;
        return {};
    }

    ChainedBuffers chain = cache_chain.empty()
        ? readSource(position_physical, max_serve)
        : readThroughCaches(position_physical, max_serve);

    const size_t got = chain.empty() ? 0 : chain.range().size;
    if (got == 0)
    {
        reached_eof = true;
        long_conn.reset();
        /// Nothing read below a known-size source's end = truncation.
        if (!offset_map.hasUnknownSize() && position < totalSize())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "ReaderExecutor: source ended at {} of {} bytes for {}",
                position, totalSize(), getFileName());
        return {};
    }
    stats.add(Stats::DeliveredBytes, got);

    chain = decryptWindow(std::move(chain));
    position += got;
    return chain;
}

ChainedBuffers ReaderExecutor::decryptWindow(ChainedBuffers && cipher)
{
    if (!needsDecryption() || cipher.empty())
        return std::move(cipher);

    /// Rebase physical->logical: source reads span the `data_start_offset` encryption header,
    /// the plaintext the caller and decryptor see starts at 0.
    cipher.shift(-static_cast<ssize_t>(data_start_offset));

    /// Nodes may alias shared cache buffers, so decrypt into fresh copies, not through them.
    StatTimer decrypt_timer(stats, Stats::DecryptMicroseconds);
    ChainedBuffers plain;
    for (const auto & node : cipher.getNodes())
    {
        auto block = std::make_shared<OwnedChainedBuffer>(node.size);
        std::memcpy(block->data(), node.data(), node.size);
        decryptInPlaceIfNeeded(block->data(), node.size, node.range().offset);
        plain.append(ChainedBufferNode{std::move(block), 0, node.size, node.range().offset});
    }
    return plain;
}

void ReaderExecutor::seek(size_t new_position)
{
    LOG_TRACE(log, "seek: {} -> {}", position, new_position);
    /// Feed the estimator; a held connection that can't continue is dropped lazily by the next
    /// `readNextWindow`.
    fetch_tracker.recordSeek(toPhysical(new_position));
    position = new_position;
    reached_eof = false;
}

}
