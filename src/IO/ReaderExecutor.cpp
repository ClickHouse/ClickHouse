#include <IO/ReaderExecutor.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/HistogramMetrics.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Interpreters/ReaderExecutorLog.h>
#include <chrono>

#include "config.h"

namespace ProfileEvents
{
    extern const Event LiveSourceBufferCreated;
    extern const Event LiveSourceBufferHits;
    extern const Event LiveSourceBufferFallbacks;
    extern const Event LiveSourceBufferBytes;
    extern const Event ReaderExecutorCacheHitBytes;
    extern const Event ReaderExecutorCacheMissBytes;
    extern const Event ReaderExecutorCachePopulatedBytes;
    extern const Event ReaderExecutorAllocatedBytes;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorCacheGetMicroseconds;
    extern const Event ReaderExecutorCachePopulateMicroseconds;
    extern const Event ReaderExecutorSourceReadMicroseconds;
    extern const Event ReaderExecutorDecryptMicroseconds;
    extern const Event ReaderExecutorPrefetchWaitMicroseconds;
    extern const Event ReaderExecutorSyncReadMicroseconds;
    extern const Event ReaderExecutorPrefetchHits;
    extern const Event ReaderExecutorPrefetchCancelled;
    extern const Event ReaderExecutorPrefetchPoolFull;
    extern const Event ReaderExecutorBufferSlotAcquired;
    extern const Event ReaderExecutorBufferSlotFailed;
    extern const Event ReaderExecutorOverReadOverflow;
    extern const Event ReaderExecutorOverReadBytes;
    extern const Event ReaderExecutorOverReadServedBytes;
}

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorActive;
}

namespace HistogramMetrics
{
    extern Metric & ReaderExecutorCacheReadLatency;
    extern Metric & ReaderExecutorCachePopulateLatency;
    extern Metric & ReaderExecutorSourceReadLatency;
    extern Metric & ReaderExecutorPrefetchWaitLatency;
    extern Metric & ReaderExecutorSyncReadLatency;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromMemory.h>
#endif

#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <algorithm>
#include <cstring>

namespace DB
{

namespace
{

/// Disjoint, sorted set of ByteRanges. Used by readPhysicalWindow to track
/// which logical bytes are already in `result` so that:
///   - cache hits are appended only for not-yet-covered subranges,
///   - source-fetched bytes are appended only for not-yet-covered subranges.
/// This makes `result` always disjoint by construction, so any `slice` over
/// it is also disjoint — which is what `cache->put` requires.
class IntervalSet
{
public:
    void add(ByteRange r)
    {
        if (r.size == 0)
            return;

        size_t new_start = r.offset;
        size_t new_end = r.end();

        auto erase_from = intervals.begin();
        while (erase_from != intervals.end() && erase_from->end() < new_start)
            ++erase_from;

        auto it = erase_from;
        while (it != intervals.end() && it->offset <= new_end)
        {
            new_start = std::min(new_start, it->offset);
            new_end = std::max(new_end, it->end());
            ++it;
        }

        auto insert_pos = intervals.erase(erase_from, it);
        intervals.insert(insert_pos, ByteRange{new_start, new_end - new_start});
    }

    /// Returns r minus all intervals in the set, as a list of disjoint
    /// sub-ranges in increasing-offset order.
    VectorWithMemoryTracking<ByteRange> subtract(ByteRange r) const
    {
        VectorWithMemoryTracking<ByteRange> out;
        if (r.size == 0)
            return out;
        size_t cur = r.offset;
        size_t end = r.end();
        for (const auto & i : intervals)
        {
            if (i.end() <= cur)
                continue;
            if (i.offset >= end)
                break;
            if (i.offset > cur)
                out.push_back({cur, i.offset - cur});
            cur = std::max(cur, i.end());
            if (cur >= end)
                break;
        }
        if (cur < end)
            out.push_back({cur, end - cur});
        return out;
    }

private:
    VectorWithMemoryTracking<ByteRange> intervals;
};

}

ReaderExecutor::ReaderExecutor(
    std::shared_ptr<ISourceReader> source_,
    const StoredObjects & objects,
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches_,
    size_t window_size_,
    size_t min_bytes_for_seek_,
    String log_file_path_)
    : source(std::move(source_))
    , stored_objects(objects)
    , caches(std::move(caches_))
    , log_file_path(std::move(log_file_path_))
    , window_size(window_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
    CurrentMetrics::add(CurrentMetrics::ReaderExecutorActive);
    offset_map.build(stored_objects);
    /// Capture in the ctor — the executor may be destroyed on a worker thread
    /// whose `CurrentThread` is no longer attached to the query, in which
    /// case `getQueryId()` would return empty at destruction time and
    /// `system.reader_executor_log` rows would be unfindable.
    creator_query_id = String(CurrentThread::getQueryId());
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, caches.size());
}

ReaderExecutor::~ReaderExecutor()
{
    discardPrefetch();
    CurrentMetrics::sub(CurrentMetrics::ReaderExecutorActive);

    /// Flush per-executor stats to global ProfileEvents (visible in
    /// `system.query_log.ProfileEvents`) and emit a single triage line.
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheHitBytes, stats.cache_hit_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheMissBytes, stats.cache_miss_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulatedBytes, stats.cache_populated_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorAllocatedBytes, stats.allocated_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetRequests, stats.cache_get_requests);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateRequests, stats.cache_populate_requests);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceRequests, stats.source_requests);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCacheGetMicroseconds, stats.cache_get_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorCachePopulateMicroseconds, stats.cache_populate_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorSourceReadMicroseconds, stats.source_read_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorDecryptMicroseconds, stats.decrypt_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchWaitMicroseconds, stats.prefetch_wait_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorSyncReadMicroseconds, stats.sync_read_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchHits, stats.prefetch_hits);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchCancelled, stats.prefetch_cancelled);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchPoolFull, stats.prefetch_pool_full);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadBytes, stats.over_read_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadServedBytes, stats.over_read_served_bytes);

    LOG_DEBUG(log,
        "Destroyed: cache_hit_bytes={} miss_bytes={} populated={} allocated={} "
        "get_reqs={} populate_reqs={} src_reqs={} "
        "get_us={} populate_us={} src_us={} decrypt_us={} "
        "prefetch_wait_us={} sync_read_us={} "
        "prefetch_hits={} prefetch_cancelled={} prefetch_pool_full={} "
        "over_read={} over_read_served={}",
        stats.cache_hit_bytes, stats.cache_miss_bytes,
        stats.cache_populated_bytes, stats.allocated_bytes,
        stats.cache_get_requests, stats.cache_populate_requests, stats.source_requests,
        stats.cache_get_us, stats.cache_populate_us,
        stats.source_read_us, stats.decrypt_us,
        stats.prefetch_wait_us, stats.sync_read_us,
        stats.prefetch_hits, stats.prefetch_cancelled, stats.prefetch_pool_full,
        stats.over_read_bytes, stats.over_read_served_bytes);

    if (reader_executor_log)
    {
        ReaderExecutorLogElement elem;
        elem.event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        elem.query_id = creator_query_id;
        elem.source_file_path = log_file_path;
        elem.total_size = offset_map.totalSize();
        elem.cache_hit_bytes = stats.cache_hit_bytes;
        elem.cache_miss_bytes = stats.cache_miss_bytes;
        elem.cache_populated_bytes = stats.cache_populated_bytes;
        elem.allocated_bytes = stats.allocated_bytes;
        elem.cache_get_requests = stats.cache_get_requests;
        elem.cache_populate_requests = stats.cache_populate_requests;
        elem.source_requests = stats.source_requests;
        elem.cache_get_us = stats.cache_get_us;
        elem.cache_populate_us = stats.cache_populate_us;
        elem.source_read_us = stats.source_read_us;
        elem.decrypt_us = stats.decrypt_us;
        elem.prefetch_wait_us = stats.prefetch_wait_us;
        elem.sync_read_us = stats.sync_read_us;
        elem.prefetch_hits = stats.prefetch_hits;
        elem.prefetch_cancelled = stats.prefetch_cancelled;
        elem.prefetch_pool_full = stats.prefetch_pool_full;
        reader_executor_log->add(std::move(elem));
    }
}

VectorWithMemoryTracking<ByteRange> ReaderExecutor::mergeRanges(const VectorWithMemoryTracking<ByteRange> & ranges, size_t min_gap)
{
    if (ranges.empty() || min_gap == 0)
        return ranges;

    VectorWithMemoryTracking<ByteRange> sorted = ranges;
    std::sort(sorted.begin(), sorted.end(),
        [](const ByteRange & a, const ByteRange & b) { return a.offset < b.offset; });

    VectorWithMemoryTracking<ByteRange> merged;
    merged.push_back(sorted[0]);

    for (size_t i = 1; i < sorted.size(); ++i)
    {
        auto & prev = merged.back();
        /// Saturating subtraction: overlapping ranges (sorted[i].offset < prev.end())
        /// collapse to gap = 0 and merge via the same branch as adjacent ranges.
        size_t gap = sorted[i].offset > prev.end() ? sorted[i].offset - prev.end() : 0;

        if (gap <= min_gap)
        {
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

void ReaderExecutor::setBufferLimit(std::shared_ptr<SourceBufferLimit> limit)
{
    buffer_limit = std::move(limit);
}

void ReaderExecutor::setReaderExecutorLog(std::shared_ptr<ReaderExecutorLog> log_)
{
    reader_executor_log = std::move(log_);
}

void ReaderExecutor::ensurePreAcquiredSlot()
{
    if (live_buffer || pre_acquired_slot || !buffer_limit)
        return;

    /// Pre-acquire for the object the next source read will actually hit, not
    /// blindly for the first object. Otherwise a seek into a later object
    /// would acquire a slot for object A that `readFromSource` can't consume
    /// (it sees object B), forcing it to acquire a *second* slot — and the
    /// first slot then stays pinned for the executor's lifetime.
    const size_t physical_position = position + data_start_offset;
    const StoredObject * object = offset_map.findObjectAt(physical_position);
    if (!object)
        return;

    auto slot = buffer_limit->tryAcquire(buffer_limit, object->remote_path, object->local_path, String(CurrentThread::getQueryId()));
    if (slot)
    {
        LOG_TRACE(log, "ensurePreAcquiredSlot: got slot for {}", object->remote_path);
        pre_acquired_slot.emplace(std::move(*slot));
        ProfileEvents::increment(ProfileEvents::ReaderExecutorBufferSlotAcquired);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::ReaderExecutorBufferSlotFailed);
    }
}

void ReaderExecutor::releaseStalePreAcquiredSlot(const String & target_path)
{
    if (pre_acquired_slot && pre_acquired_slot->objectPath() != target_path)
        pre_acquired_slot.reset();
}

namespace
{

/// Level → (window, block) table. Lives with the reader (the only consumer
/// of these sizes) — the monitor itself is size-agnostic. Indexed by
/// `static_cast<size_t>(MemoryPressureLevel)`.
struct WindowAndBlock
{
    size_t window_bytes;
    size_t block_bytes;
};

constexpr WindowAndBlock LEVEL_SIZES[memoryPressureLevelCount()] = {
    {8ULL << 20,   1ULL << 20  },  // Normal
    {2ULL << 20,   512ULL << 10},  // Elevated
    {512ULL << 10, 512ULL << 10},  // High
    {128ULL << 10, 128ULL << 10},  // Critical
};

WindowAndBlock sizesAtCurrentPressure()
{
    return LEVEL_SIZES[static_cast<size_t>(memoryPressureMonitor().currentLevel())];
}

}

size_t ReaderExecutor::effectiveWindowSize() const
{
    /// Live path streams one block at a time via the live buffer's next() — a
    /// larger window just inflates the in-flight rope without any throughput
    /// gain. Cap to `effectiveBlockSize()` in that mode but never grow past
    /// the caller-configured `window_size` (tests pin it small on purpose).
    /// Stateless path benefits from the larger batch read to amortise the
    /// HTTP setup cost of a fresh underlying buffer.
    ///
    /// Both paths additionally clamp against the current memory-pressure
    /// level — under memory pressure the executor shrinks per-call
    /// allocations before the server's hard limit fires.
    const size_t pressure_window = sizesAtCurrentPressure().window_bytes;
    if (live_buffer || pre_acquired_slot)
        return std::min({window_size, effectiveBlockSize(), pressure_window});
    return std::min(window_size, pressure_window);
}

size_t ReaderExecutor::effectiveBlockSize() const
{
    const size_t pressure_block = sizesAtCurrentPressure().block_bytes;
    return std::min(ROPE_BLOCK_SIZE, pressure_block);
}

void ReaderExecutor::maybeTriggerPrefetch()
{
    if (!prefetch_pool || prefetch_handle || reached_eof)
        return;

    size_t logical_size = totalSize();
    /// Size-known: bail at EOF. Size-unknown: `logical_size == MAX` so
    /// `position >= logical_size` never fires; we instead rely on
    /// `reached_eof` (checked above) and the fact that
    /// `effectiveWindowSize()` doesn't depend on `logical_size`.
    if (!offset_map.hasUnknownSize() && position >= logical_size)
        return;

    ensurePreAcquiredSlot();
    size_t next_size = offset_map.hasUnknownSize()
        ? effectiveWindowSize()
        : std::min(effectiveWindowSize(), logical_size - position);
    ByteRange next_physical_window{position + data_start_offset, next_size};
    size_t next_logical_offset = position;

    LOG_TRACE(log, "Prefetch: submitting physical [{}, {})", next_physical_window.offset, next_physical_window.end());

    auto handle = prefetch_pool->submit([this, next_physical_window, next_logical_offset]()
    {
        return decryptRope(readPhysicalWindow(next_physical_window), next_logical_offset);
    });

    if (!handle)
    {
        /// Prefetch pool's queue is full. The next readNextWindow will do a
        /// synchronous fetch — that's the correct fallback under overload.
        LOG_TRACE(log, "Prefetch: pool queue full, will fetch synchronously on next read");
        ++stats.prefetch_pool_full;
        return;
    }

    prefetch_handle = std::move(handle);
    /// Track prefetch_range in logical coordinates — same space as `position`
    /// and as the decrypted rope returned by the handle.
    prefetch_range = ByteRange{next_logical_offset, next_size};
}

void ReaderExecutor::discardPrefetch()
{
    /// Take ownership of the handle BEFORE we touch it. If a previous
    /// `readNextWindow` already consumed the future (e.g. by throwing the
    /// worker's exception up the stack without resetting `prefetch_handle`),
    /// the future's internal `__state_` pointer is already null and a second
    /// `get()` would segfault inside `pthread_mutex_lock`. Moving out first
    /// guarantees we never call `get()` on a half-consumed handle from the
    /// destructor.
    auto local_handle = std::move(prefetch_handle);
    if (!local_handle)
        return;

    LOG_TRACE(log, "Prefetch: discarding [{}, {})", prefetch_range.offset, prefetch_range.end());

    {
        /// If the task hadn't started, cancel it — no need to wait. Otherwise
        /// we must wait for the worker to finish so its capture of `this` is
        /// safe to drop.
        if (!local_handle->tryCancel())
        {
            try
            {
                std::ignore = local_handle->get();
            }
            catch (...)
            {
                /// We're discarding the prefetch — either from the destructor (where
                /// throwing is forbidden) or from seek (where the lambda's failure is
                /// orthogonal to the seek). Log and swallow either way; the next
                /// readNextWindow will surface a fresh exception if the underlying
                /// I/O is still broken.
                tryLogCurrentException(log, "Discarded prefetch task threw");
            }
        }
    }
}

void ReaderExecutor::addDecryptionLayer(
    [[maybe_unused]] String path,
    [[maybe_unused]] size_t buffer_size,
    [[maybe_unused]] KeyFinderFunc key_finder)
{
#if USE_SSL
    decryption_layers.push_back(DecryptionLayer{
        .path = std::move(path),
        .buffer_size = buffer_size,
        .key_finder = std::move(key_finder),
        .key = {},
    });
    data_start_offset = decryption_layers.size() * FileEncryption::Header::kSize;
    LOG_DEBUG(log, "Added decryption layer, data_start_offset={}", data_start_offset);
#endif
}

void ReaderExecutor::initDecryption()
{
#if USE_SSL
    if (decryption_initialized || decryption_layers.empty())
        return;

    size_t total_source_size = offset_map.totalSize();

    /// An empty underlying source (e.g. DiskObjectStorage's empty-file
    /// fallback for paths with no storage objects) has no encryption header.
    /// Skip — subsequent reads will return 0 bytes, matching the contract of
    /// reading an empty file on an unencrypted disk.
    if (total_source_size == 0)
    {
        LOG_DEBUG(log, "initDecryption: source is empty, skipping");
        return;
    }

    /// Source exists but is smaller than the header(s) — file is corrupted.
    if (total_source_size < data_start_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Encrypted source has {} bytes, less than header size {}",
            total_source_size, data_start_offset);

    LOG_DEBUG(log, "initDecryption: reading {} headers ({} bytes)",
        decryption_layers.size(), data_start_offset);

    /// Read all headers in one call through the cache chain.
    Rope header_rope = readPhysicalWindow(ByteRange{0, data_start_offset});
    chassert(header_rope.totalBytes() == data_start_offset);

    /// Parse each header sequentially from the rope.
    size_t offset = 0;
    for (auto & layer : decryption_layers)
    {
        Rope one_header = header_rope.slice(ByteRange{offset, FileEncryption::Header::kSize});
        chassert(one_header.totalBytes() == FileEncryption::Header::kSize);

        /// Header is 64 bytes — fits in a single node after slice.
        const auto & nodes = one_header.getNodes();
        chassert(nodes.size() == 1);
        ReadBufferFromMemory rb(nodes[0].data(), nodes[0].size);

        FileEncryption::Header header;
        header.read(rb);
        layer.key = layer.key_finder(header.key_fingerprint, layer.path);
        decryption_headers.push_back(std::move(header));
        offset += FileEncryption::Header::kSize;

        LOG_DEBUG(log, "initDecryption: parsed header for {}, algorithm={}",
            layer.path, static_cast<int>(decryption_headers.back().algorithm));
    }

    decryption_initialized = true;
#endif
}

Rope ReaderExecutor::decryptRope(Rope rope, [[maybe_unused]] size_t logical_offset) const
{
#if USE_SSL
    if (decryption_layers.empty())
        return rope;

    const size_t total = rope.totalBytes();
    if (total == 0)
        return {};

    /// Mirror the reading-side iteration pattern: one block at a time.
    ///
    /// The naive shape — allocate all destination blocks, copy the whole input
    /// rope into them, decrypt across all blocks — doubles peak memory for the
    /// duration of this call. We instead stream block-by-block:
    ///   1. Allocate one output block.
    ///   2. Copy this block's encrypted bytes out of the front of `rope`.
    ///   3. Decrypt in place across all layers.
    ///   4. Append to result and `advance` the input rope so its
    ///      `OwnedRopeBuffer`s can be released if we hold the only reference.
    ///
    /// CTR mode is fully position-addressable, so `setOffset(logical_offset +
    /// pos)` gives the same keystream as decrypting the whole range at once.

    /// Encryptors are reused across blocks — cheaper than constructing one
    /// per block per layer.
    VectorWithMemoryTracking<FileEncryption::Encryptor> encryptors;
    encryptors.reserve(decryption_layers.size());
    for (size_t i = 0; i < decryption_layers.size(); ++i)
        encryptors.emplace_back(
            decryption_headers[i].algorithm,
            decryption_layers[i].key,
            decryption_headers[i].init_vector);

    Rope result;
    const size_t block = effectiveBlockSize();
    size_t pos = 0;
    while (pos < total)
    {
        const size_t chunk = std::min(block, total - pos);
        auto buf = std::make_shared<OwnedRopeBuffer>(chunk);

        /// Drain `chunk` bytes from the front of `rope` via peek/advance.
        /// This avoids reasoning about the physical-vs-logical offset gap
        /// (the encryption header at `data_start_offset`) — the cursor
        /// just walks the encrypted bytes in order, regardless of which
        /// absolute file offset they happen to live at.
        size_t out_pos = 0;
        while (out_pos < chunk)
        {
            auto span = rope.peek();
            chassert(span.size > 0);
            const size_t take = std::min(span.size, chunk - out_pos);
            std::memcpy(buf->data() + out_pos, span.data, take);
            rope.advance(take);
            out_pos += take;
        }

        for (auto & enc : encryptors)
        {
            enc.setOffset(logical_offset + pos);
            enc.decrypt(buf->data(), chunk, buf->data());
        }

        result.append(RopeNode{buf, 0, chunk, logical_offset + pos});
        pos += chunk;
    }
    return result;
#else
    return rope;
#endif
}

Rope ReaderExecutor::readNextWindow()
{
    size_t logical_size = totalSize();
    Rope rope;

    if (prefetch_handle)
    {
        /// Consume the prefetched rope BEFORE applying the EOF gate. For an
        /// unknown-size source the prefetch worker can set `reached_eof`
        /// from inside `readPhysicalWindow` (short return) while still
        /// returning a partial rope with the real final bytes. Treating
        /// `reached_eof` as authoritative here would `discardPrefetch` and
        /// drop those bytes — they would never be served to the caller.
        /// `maybeTriggerPrefetch` already refuses to schedule another
        /// prefetch once `reached_eof` is set, so the next call lands in
        /// the no-prefetch branch and short-circuits to EOF correctly.
        /// Take ownership of the handle BEFORE calling `tryCancel` / `get`.
        /// `std::future::get` detaches the future's associated state as its
        /// very first step (even if the worker stored an exception), so a
        /// thrown `get` would leave `prefetch_handle` non-null but pointing
        /// at an already-consumed future. A later
        /// `~ReaderExecutor → discardPrefetch → get` would then segfault
        /// dereferencing the detached null state pointer (the crash we saw
        /// in stress test as a SIGSEGV at offset 0x28, the
        /// `__assoc_state::__mut_` slot).
        auto local_handle = std::move(prefetch_handle);

        if (local_handle->tryCancel())
        {
            /// Worker hadn't picked up the task — cancel and read from the
            /// current position with a full window. If a seek landed inside
            /// the prefetched range we'd otherwise re-read the pre-seek bytes
            /// only to discard them; starting at `position` instead lets us
            /// return a full window of useful data.
            LOG_TRACE(log, "readNextWindow: prefetch was queued, cancelling and reading from position {}", position);
            ++stats.prefetch_cancelled;

            ensurePreAcquiredSlot();
            size_t win_size = offset_map.hasUnknownSize()
                ? effectiveWindowSize()
                : std::min(effectiveWindowSize(), logical_size - position);
            ByteRange physical_window{position + data_start_offset, win_size};
            StopwatchAccumulator sync_scope(stats.sync_read_us);
            rope = decryptRope(readPhysicalWindow(physical_window), position);
            HistogramMetrics::ReaderExecutorSyncReadLatency.observe(
                static_cast<HistogramMetrics::Value>(sync_scope.elapsedMicroseconds()));
        }
        else
        {
            /// Worker already running or done — wait. If a seek landed inside
            /// the prefetched window, trim the prefix so rope.range().offset
            /// matches `position`.
            LOG_TRACE(log, "readNextWindow: waiting on prefetched [{}, {})", prefetch_range.offset, prefetch_range.end());
            StopwatchAccumulator wait_scope(stats.prefetch_wait_us);
            rope = local_handle->get();
            ++stats.prefetch_hits;
            HistogramMetrics::ReaderExecutorPrefetchWaitLatency.observe(
                static_cast<HistogramMetrics::Value>(wait_scope.elapsedMicroseconds()));

            if (!rope.empty() && position > rope.range().offset)
            {
                size_t end = rope.range().end();
                rope = rope.slice(ByteRange{position, end - position});
            }
        }
    }
    else
    {
        /// EOF detection has two flavours:
        ///   1. Size-known: `position >= totalSize` is authoritative.
        ///   2. Size-unknown (`offset_map.hasUnknownSize()`): we can only
        ///      learn EOF from the source returning fewer bytes than asked
        ///      for. `reached_eof` is set in the read path when that happens
        ///      and consulted here so subsequent calls don't re-issue a
        ///      doomed read.
        /// The pending-prefetch branch above intentionally bypasses this
        /// gate — see its comment for the unknown-size race.
        const bool size_known_eof = !offset_map.hasUnknownSize() && position >= logical_size;
        if (size_known_eof || reached_eof)
        {
            LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
            /// Release scarce per-stream resources as soon as the caller
            /// hits EOF — there's no reason to hold an open connection /
            /// `SourceBufferLimit` slot just because the caller hasn't
            /// dropped the `PipelineReadBuffer` yet. If they later seek
            /// back and read again, we'll re-open and re-acquire.
            live_buffer.reset();
            pre_acquired_slot.reset();
            over_read_buffer = Rope{};
            return {};
        }

        ensurePreAcquiredSlot();
        size_t win_size = std::min(effectiveWindowSize(), logical_size - position);
        ByteRange physical_window{position + data_start_offset, win_size};
        LOG_TRACE(log, "readNextWindow: synchronous read physical [{}, {}), logical [{}, {})",
            physical_window.offset, physical_window.end(), position, position + win_size);
        StopwatchAccumulator sync_scope(stats.sync_read_us);
        rope = decryptRope(readPhysicalWindow(physical_window), position);
        HistogramMetrics::ReaderExecutorSyncReadLatency.observe(
            static_cast<HistogramMetrics::Value>(sync_scope.elapsedMicroseconds()));
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

    if (prefetch_handle
        && new_position >= prefetch_range.offset
        && new_position < prefetch_range.end())
    {
        LOG_TRACE(log, "seek: target within prefetch [{}, {}), keeping prefetch",
            prefetch_range.offset, prefetch_range.end());
        position = new_position;
        return;
    }

    discardPrefetch();
    /// Pre-acquired slot is keyed to the old position's object. If the new
    /// position lies in a different object (or no object at all), drop the
    /// slot so the next read doesn't acquire a second slot while the stale
    /// one stays held. Pairs with the path-mismatch reset in `readFromSource`.
    if (const auto * new_obj = offset_map.findObjectAt(new_position + data_start_offset))
        releaseStalePreAcquiredSlot(new_obj->remote_path);
    else
        releaseStalePreAcquiredSlot(String{});
    /// Drop the over-read buffer on seek: the live connection is about to be
    /// invalidated anyway (the next source-read won't continue from
    /// live_buffer's position), so the speculative bytes lose their pairing
    /// with the live connection and we can't reuse them productively.
    over_read_buffer = {};
    position = new_position;
    /// Clear the unknown-size EOF latch: a seek backward might land in a
    /// region the source can re-deliver. We learn EOF again from the next
    /// short return.
    reached_eof = false;

    /// Start prefetching the new window right away so the next `readNextWindow`
    /// can hit a warm prefetch instead of paying full source-read latency
    /// synchronously. `maybeTriggerPrefetch` is a no-op when there is no
    /// `prefetch_pool` (transient `readBigAt` executor) or when we are already
    /// at EOF, so this is safe to call unconditionally.
    maybeTriggerPrefetch();
}

VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> ReaderExecutor::allocateBlocks(
    size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits)
{
    chassert(block_size > 0);
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks;
    blocks.reserve((size + block_size - 1) / block_size + splits.size());

    size_t pos = 0;
    auto split_it = splits.begin();
    while (pos < size)
    {
        /// Advance past any splits already passed.
        while (split_it != splits.end() && *split_it <= pos)
            ++split_it;

        /// Next mandatory boundary: either the next split or the end.
        const size_t boundary = (split_it != splits.end()) ? std::min(*split_it, size) : size;
        const size_t chunk = std::min(block_size, boundary - pos);
        blocks.push_back(std::make_shared<OwnedRopeBuffer>(chunk));
        pos += chunk;
    }
    return blocks;
}

/// Read up to `chunk` bytes into `dest`. Uses zero-copy set()+next() when the
/// underlying buffer honors the external-buffer pointer (synchronous impls);
/// falls back to explicit read() for impls that do not (asynchronous readers
/// like pread_threadpool/io_uring read into their own allocation and assume
/// memory.size() == internal_buffer.size(), so set()+next() would corrupt
/// the heap whenever chunk exceeds the buffer's constructor-time size).
/// Returns bytes read; 0 means EOF. May return less than `chunk` only when
/// the underlying source reached EOF — short positive `next` returns are
/// looped over so the caller (readPhysicalWindow) does not misinterpret a
/// partial fill as `actual < pr.size` and spuriously throw
/// `CANNOT_READ_ALL_DATA` (size-known) or latch `reached_eof`
/// (size-unknown) when more bytes are still available.
static size_t readIntoBlock(ReadBuffer & buf, char * dest, size_t chunk)
{
    if (buf.supportsExternalBufferMode())
    {
        size_t total = 0;
        while (total < chunk)
        {
            /// Re-arm the external buffer for the still-unfilled tail. The
            /// source's internal position has already advanced by `total`
            /// from the previous iteration, so each `next` reads into the
            /// next slot in `dest` and the bytes land contiguously.
            buf.set(dest + total, chunk - total);
            if (!buf.next())
                break;
            size_t got = buf.available();
            if (got == 0)
                break;  /// Defensive: source returned `true` with no data.
            buf.position() = buf.buffer().end();
            total += got;
        }
        return total;
    }

    /// Fallback: copy out of the buffer's own memory. `ReadBuffer::read`
    /// already loops internally via `!eof()`.
    return buf.read(dest, chunk);
}

Rope ReaderExecutor::readFromLiveBufferIntoRope(
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset)
{
    chassert(live_buffer);
    auto & buf = *live_buffer->buffer;

    Rope rope;
    size_t total_read = 0;

    for (auto & block : blocks)
    {
        size_t chunk = block->size();
        size_t got = readIntoBlock(buf, block->data(), chunk);

        LOG_DEBUG(log, "readFromLiveBufferIntoRope: block {}, chunk={}, got={}, first_byte=0x{:02x}",
            rope.getNodes().size(), chunk, got,
            got > 0 ? static_cast<unsigned char>(block->data()[0]) : 0);

        if (got == 0)
            break;

        rope.append(RopeNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    /// Blocks not referenced from rope (file ended before consuming all of them)
    /// drop their refcount to 0 when `blocks` goes out of scope here.
    return rope;
}

Rope ReaderExecutor::readFromSource(
    const StoredObject & object, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset)
{
    /// Try live buffer: reuse open connection for sequential reads.
    if (live_buffer
        && live_buffer->slot.objectPath() == object.remote_path
        && live_buffer->current_position == offset)
    {
        LOG_TRACE(log, "readFromSource: live buffer hit for {}, position={}", object.remote_path, offset);
        ProfileEvents::increment(ProfileEvents::LiveSourceBufferHits);

        Rope rope = readFromLiveBufferIntoRope(std::move(blocks), logical_offset);
        size_t total_read = rope.totalBytes();

        ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
        live_buffer->current_position += total_read;
        live_buffer->slot.updatePosition(live_buffer->current_position);
        return rope;
    }

    /// Live buffer doesn't match — close it if present.
    if (live_buffer)
    {
        LOG_TRACE(log, "readFromSource: closing live buffer for {} (was at {}), need {}:{}",
            live_buffer->slot.objectPath(), live_buffer->current_position, object.remote_path, offset);
        live_buffer.reset();
    }

    /// Pre-acquired slot keyed to a different object — drop it. Otherwise
    /// the matching-path branch below would skip it and the `else if
    /// (buffer_limit)` fallback would acquire a SECOND slot while the stale
    /// one stays held until executor destruction.
    releaseStalePreAcquiredSlot(object.remote_path);

    /// Try to acquire a slot for a new live buffer. If `pre_acquired_slot`
    /// was set by the caller (readNextWindow / maybeTriggerPrefetch) and
    /// matches this object, reuse it instead of asking buffer_limit again.
    std::optional<SourceBufferSlot> slot;
    if (pre_acquired_slot && pre_acquired_slot->objectPath() == object.remote_path)
    {
        slot.emplace(std::move(*pre_acquired_slot));
        pre_acquired_slot.reset();
    }
    else if (buffer_limit)
    {
        slot = buffer_limit->tryAcquire(buffer_limit, object.remote_path, object.local_path, String(CurrentThread::getQueryId()));
    }

    if (slot)
    {
        auto opened = source->open(object);
        if (opened)
        {
            if (offset > 0)
                opened->seek(offset, SEEK_SET);

            live_buffer.emplace(LiveBuffer{
                .current_position = offset,
                .buffer = std::move(opened),
                .slot = std::move(*slot),
            });
            ++stats.source_requests;

            Rope rope = readFromLiveBufferIntoRope(std::move(blocks), logical_offset);
            size_t total_read = rope.totalBytes();

            live_buffer->current_position += total_read;
            live_buffer->slot.updatePosition(live_buffer->current_position);

            ProfileEvents::increment(ProfileEvents::LiveSourceBufferCreated);
            ProfileEvents::increment(ProfileEvents::LiveSourceBufferBytes, total_read);
            LOG_TRACE(log, "readFromSource: opened live buffer for {}, read {} bytes, position={}",
                object.remote_path, total_read, live_buffer->current_position);
            return rope;
        }
        /// open() failed — `slot` drops here, releasing the buffer_limit reservation.
    }

    /// Fallback: open a fresh connection without storing it as live_buffer
    /// (slot was unavailable). The opened buffer is dropped when this
    /// function returns.
    ProfileEvents::increment(ProfileEvents::LiveSourceBufferFallbacks);

    auto opened = source->open(object);
    if (offset > 0)
        opened->seek(offset, SEEK_SET);
    auto & buf = *opened;
    ++stats.source_requests;

    Rope rope;
    size_t total_read = 0;

    for (auto & block : blocks)
    {
        size_t chunk = block->size();
        size_t got = readIntoBlock(buf, block->data(), chunk);

        LOG_DEBUG(log, "readFromSource: stateless block offset={}, chunk={}, got={}, first_byte=0x{:02x}",
            offset + total_read, chunk, got,
            got > 0 ? static_cast<unsigned char>(block->data()[0]) : 0);

        if (got == 0)
            break;

        rope.append(RopeNode{block, 0, got, logical_offset + total_read});
        total_read += got;
    }

    /// Blocks not referenced from rope drop their refcount to 0 when this function returns.
    return rope;
}

Rope ReaderExecutor::readPhysicalWindow(ByteRange physical_window)
{
    LOG_TRACE(log, "readPhysicalWindow [{}, {})", physical_window.offset, physical_window.end());

    Rope result;
    IntervalSet covered;   /// disjoint logical bytes already materialised in `result`
    /// Both vectors hold cache handles until the end of this function so
    /// `~ICacheHandle` (which does the deferred LRU bump for hits) runs
    /// AFTER every `put` below. Splitting on whether the handle has misses
    /// (so we know which to iterate for `put`) is a layout detail — both
    /// are destroyed together in scope exit. See `~DiskCacheHandle` for the
    /// deferred-bump rationale.
    VectorWithMemoryTracking<std::unique_ptr<ICacheHandle>> miss_handles;
    VectorWithMemoryTracking<std::unique_ptr<ICacheHandle>> hit_only_handles;

    /// Over-read pre-check: bytes the previous `readPhysicalWindow` source-read
    /// just past its requested window (with `live_buffer` advancing through
    /// them) and we retained in their own `OwnedRopeBuffer`. Serving them here
    /// lets the live buffer reuse kick in for the remainder — the source-read
    /// will start at the exact byte where live_buffer is positioned.
    /// We move the served slices into `result` and immediately drop them from
    /// `over_read_buffer` (the only future use was the current call), keeping
    /// only the still-future-relevant tail.
    if (!over_read_buffer.empty())
    {
        /// Two-step trim via `Rope::slice`:
        ///   1. Intersection with `physical_window` -> serve into `result`.
        ///      Don't bump `cache_hit_bytes`: these bytes were already
        ///      accounted as `cache_miss_bytes` in the source-read that
        ///      produced them.
        ///   2. Anything strictly after `physical_window.end()` -> retain as
        ///      the new `over_read_buffer`. Anything before it is dropped
        ///      (sequential reads move forward; `seek` would have cleared
        ///      the buffer already).
        Rope served = over_read_buffer.slice(physical_window);
        for (const auto & node : served.getNodes())
            covered.add(ByteRange{node.logical_offset, node.size});
        const size_t served_bytes = served.totalBytes();
        stats.over_read_served_bytes += served_bytes;
        LOG_TRACE(log, "readPhysicalWindow: over-read served {} bytes", served_bytes);
        result.append(std::move(served));

        /// Half-open `[physical_window.end(), +∞)` — using a sufficiently
        /// large size avoids overflow (`ByteRange::end()` does `offset + size`).
        const size_t tail_size = std::numeric_limits<size_t>::max() / 2;
        over_read_buffer = over_read_buffer.slice(
            ByteRange{physical_window.end(), tail_size});
    }

    /// Walk the cache chain. Each cache reports hits/misses at its own
    /// granularity (which may extend beyond the requested range). Hits are
    /// appended to `result` only for the not-yet-covered subranges so that
    /// `result` stays disjoint even when a lower cache's hit overlaps a
    /// higher cache's hit. Misses propagate to the next cache.
    /// Only walk the cache chain for parts not already served by over-read.
    /// (If `covered` is empty, this returns the whole window; if over_read
    /// covered everything, returns empty and the cache+source loops below are
    /// no-ops.)
    VectorWithMemoryTracking<ByteRange> remaining = covered.subtract(physical_window);
    for (auto & cache : caches)
    {
        VectorWithMemoryTracking<ByteRange> still_missing;

        for (const auto & r : remaining)
        {
            /// Split `r` by object boundaries so each `cache->lookup` carries
            /// a single `StoredObject` and the cache provider can derive a
            /// per-object key / origin (etag-keyed cache identity, segment
            /// key-type classification, ...). The handles still report
            /// hits / misses in FILE-LEVEL coordinates — the per-object
            /// translation lives entirely inside the cache provider.
            auto pieces = offset_map.map(r);
            size_t piece_file_start = r.offset;
            for (const auto & pr : pieces)
            {
                const size_t object_file_offset = piece_file_start - pr.object_offset;
                ByteRange piece_range{piece_file_start, pr.size};

                auto handle = cache->lookup(pr.object, object_file_offset, piece_range);
                auto status = handle->status();
                bool any_hit_done = false;

                for (const auto & hit : status.hit_ranges)
                {
                    LOG_TRACE(log, "readPhysicalWindow: cache {} hit [{}, {})",
                        cache->name(), hit.offset, hit.end());

                    /// `hit` is the cache's *segment* range (typically
                    /// `max_file_segment_size`, default 4 MiB). Our actual
                    /// request is `piece_range`. Clamp to the intersection so
                    /// `handle->get` doesn't allocate buffer memory for
                    /// segment bytes outside the piece. Miss ranges are
                    /// intentionally left segment-sized: the wider miss is
                    /// what enables the next cache layer (or the source) to
                    /// fully populate this cache via `put`.
                    size_t lo = std::max(hit.offset, piece_range.offset);
                    size_t hi = std::min(hit.end(), piece_range.end());
                    if (lo >= hi)
                        continue;
                    ByteRange clamped{lo, hi - lo};

                    auto useful = covered.subtract(clamped);
                    if (useful.empty())
                        continue;
                    ++stats.cache_get_requests;
                    StopwatchAccumulator get_scope(stats.cache_get_us);
                    Rope hit_rope = handle->get(clamped);
                    HistogramMetrics::ReaderExecutorCacheReadLatency.observe(
                        static_cast<HistogramMetrics::Value>(get_scope.elapsedMicroseconds()));
                    stats.allocated_bytes += hit_rope.totalBytes();
                    for (const auto & sub : useful)
                    {
                        result.append(hit_rope.extract(sub));
                        covered.add(sub);
                        stats.cache_hit_bytes += sub.size;
                    }
                    any_hit_done = true;
                }

                for (const auto & miss : status.miss_ranges)
                {
                    LOG_TRACE(log, "readPhysicalWindow: cache {} miss [{}, {})",
                        cache->name(), miss.offset, miss.end());
                    still_missing.push_back(miss);
                }

                /// Keep the handle alive until the end of `readPhysicalWindow`
                /// — `~DiskCacheHandle` does the deferred LRU bump for each
                /// hit served by `get`, and we want that to fire AFTER every
                /// `put` below. If the handle has neither hits nor misses we
                /// could drop it now, but keeping all handles together costs
                /// nothing.
                if (!status.miss_ranges.empty())
                    miss_handles.push_back(std::move(handle));
                else if (any_hit_done)
                    hit_only_handles.push_back(std::move(handle));

                piece_file_start += pr.size;
            }
        }

        remaining = std::move(still_missing);
        if (remaining.empty())
            break;
    }

    /// Merge close-together miss ranges into fewer source requests. The merge
    /// may bridge across already-covered hits — the latency saving outweighs
    /// the small extra bandwidth, and the overlap is dropped below when only
    /// the uncovered portion of each source range is appended to `result`.
    auto fetch_ranges = mergeRanges(remaining, min_bytes_for_seek);

    if (fetch_ranges.size() < remaining.size())
        LOG_TRACE(log, "readPhysicalWindow: merged {} miss ranges into {} fetch ranges (min_gap={})",
            remaining.size(), fetch_ranges.size(), min_bytes_for_seek);

    /// Fetch from source. Allocate destination blocks before opening
    /// connections; try the live buffer for sequential reads and fall back
    /// to stateless reads otherwise.
    for (const auto & fr : fetch_ranges)
    {
        auto physical_ranges = offset_map.map(fr);
        size_t logical_pos = fr.offset;

        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "readPhysicalWindow: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);

            /// Split the allocation at the user-window edges so user-data
            /// blocks and over-read blocks live in separate `OwnedRopeBuffer`s.
            /// When the caller drops the returned rope, the user-data buffer's
            /// refcount drops to zero immediately — even if we retain
            /// over-read blocks in `over_read_buffer` for a later request.
            VectorWithMemoryTracking<size_t> splits;
            const size_t pr_lo = logical_pos;
            const size_t pr_hi = logical_pos + pr.size;
            if (physical_window.offset > pr_lo && physical_window.offset < pr_hi)
                splits.push_back(physical_window.offset - pr_lo);
            if (physical_window.end() > pr_lo && physical_window.end() < pr_hi)
                splits.push_back(physical_window.end() - pr_lo);
            std::sort(splits.begin(), splits.end());

            auto blocks = allocateBlocks(pr.size, effectiveBlockSize(), splits);
            stats.allocated_bytes += pr.size;
            StopwatchAccumulator src_scope(stats.source_read_us);
            Rope source_rope = readFromSource(pr.object, pr.object_offset, std::move(blocks), logical_pos);
            HistogramMetrics::ReaderExecutorSourceReadLatency.observe(
                static_cast<HistogramMetrics::Value>(src_scope.elapsedMicroseconds()));
            size_t actual = source_rope.totalBytes();
            stats.cache_miss_bytes += actual;
            /// Short-return handling:
            ///   - Size known: `offset_map.totalSize` is authoritative.
            ///     A short read means the source can't deliver bytes the
            ///     map already promised — failing fast is the only safe
            ///     choice (otherwise subsequent logical offsets would
            ///     shift, or `readNextWindow` would loop with
            ///     `position` stuck).
            ///   - Size unknown: the short return IS the EOF marker.
            ///     We record it via `reached_eof`, treat the request
            ///     range as covering only the actual bytes, and let
            ///     subsequent calls return empty.
            if (actual != pr.size)
            {
                if (!offset_map.hasUnknownSize())
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "ReaderExecutor: short read from {} at offset {}: requested {} bytes, got {}",
                        pr.object.remote_path, pr.object_offset, pr.size, actual);
                reached_eof = true;
            }

            /// Append only the parts of source_rope not already in `result`.
            /// Use `actual` (not `pr.size`) so the recorded range matches what
            /// the source actually delivered — relevant only when
            /// `reached_eof` is set, since otherwise `actual == pr.size`.
            ByteRange pr_range{logical_pos, actual};
            auto uncovered = covered.subtract(pr_range);
            for (const auto & sub : uncovered)
            {
                result.append(source_rope.extract(sub));
                covered.add(sub);
            }

            /// Retain over-read (bytes outside `physical_window`) only when the
            /// source-read advanced the live connection past this pr — otherwise
            /// the connection is closed and the bytes can't pair with a
            /// continuation on the next call. We collect the bytes that fell
            /// before AND after `physical_window` via two `slice` calls.
            const bool live_used = live_buffer.has_value()
                && live_buffer->slot.objectPath() == pr.object.remote_path
                && live_buffer->current_position == pr.object_offset + pr.size;
            if (live_used)
            {
                /// Only the tail (bytes past `physical_window.end()`) is worth
                /// retaining. A pre-window prefix would already be behind the
                /// next call's `physical_window.offset` and would be dropped by
                /// the over-read pre-check anyway (sequential reads move
                /// forward).
                Rope tail = source_rope.slice(
                    ByteRange{physical_window.end(), std::numeric_limits<size_t>::max() / 2});
                stats.over_read_bytes += tail.totalBytes();
                over_read_buffer.append(std::move(tail));
            }

            logical_pos += pr.size;
        }
    }

    /// Fill caches with disjoint slices of `result`. `result.slice(miss)` is
    /// disjoint by construction (every byte was appended via the covered
    /// guard), so `put` always receives a rope whose totalBytes == miss.size
    /// when the source covered the whole miss range, or less at EOF.
    for (auto & handle : miss_handles)
    {
        auto status = handle->status();
        for (const auto & miss : status.miss_ranges)
        {
            auto slice = result.slice(miss);
            if (slice.empty())
                continue;
            ++stats.cache_populate_requests;
            StopwatchAccumulator put_scope(stats.cache_populate_us);
            stats.cache_populated_bytes += handle->put(miss, std::move(slice));
            HistogramMetrics::ReaderExecutorCachePopulateLatency.observe(
                static_cast<HistogramMetrics::Value>(put_scope.elapsedMicroseconds()));
        }
    }

    /// `Rope` maintains sorted node order on every `append`, so no explicit
    /// sort is needed here.

    /// Overflow guard: if the retained over-read exceeds `window_size`, the
    /// access pattern isn't matching what we speculated (callers aren't
    /// consuming our lookahead within a window). Drop both the buffer and the
    /// live connection — keeping live_buffer would just delay the inevitable
    /// reset (the next call needs bytes before its position anyway). The
    /// `ReaderExecutorOverReadOverflow` ProfileEvent tracks how often this
    /// fires so a steady stream of overflows surfaces in `system.query_log`.
    if (over_read_buffer.totalBytes() > window_size)
    {
        LOG_TRACE(log, "readPhysicalWindow: over-read buffer overflow ({} > {}), dropping it and live_buffer",
            over_read_buffer.totalBytes(), window_size);
        ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadOverflow);
        over_read_buffer = {};
        live_buffer.reset();
    }

    /// Trim to the originally requested physical window.
    auto sliced = result.slice(physical_window);

    /// Hard-check the contiguity invariant on what we hand back to the
    /// caller. Bytes returned by `readPhysicalWindow` must form a single
    /// contiguous run starting at `physical_window.offset` — anything else
    /// would mean a hole in the data the caller is about to interpret as
    /// consecutive file content. Under known-size reads the run covers
    /// the full window; under unknown-size with EOF, it may end early
    /// (still contiguous from the front). This catches any regression
    /// where a cache layer, source short-read, or merge logic leaves the
    /// assembled rope with a gap.
    const auto & ivs = sliced.getIntervals();
    if (ivs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReaderExecutor::readPhysicalWindow: assembled result has {} disjoint intervals in window [{}, {}) - expected at most one contiguous run",
            ivs.size(), physical_window.offset, physical_window.end());
    if (!ivs.empty() && ivs[0].offset != physical_window.offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ReaderExecutor::readPhysicalWindow: assembled result starts at {} but window begins at {} - missing prefix bytes",
            ivs[0].offset, physical_window.offset);
    return sliced;
}

std::unique_ptr<ReaderExecutor> ReaderExecutor::makeTransientForReadAt(size_t start_position) const
{
    /// Shared with the parent (immutable or thread-safe to share):
    ///   - `source` (concurrent `open()` is OK; required by `canReadAt()`)
    ///   - `stored_objects`, `cache_key`, `window_size`, `min_bytes_for_seek`
    ///   - `caches` (each `ICacheProvider` is internally thread-safe)
    ///   - `decryption_layers`, `decryption_headers`, `data_start_offset`
    ///     (set once in `initDecryption`, never mutated)
    ///   - `buffer_limit`: shared so the transient's live connection counts
    ///     against the server-wide budget. Within one `readBigAt` the
    ///     transient keeps its slot for the whole request and reuses the
    ///     open connection across windows; on slot exhaustion it falls back
    ///     to one-shot reads, same as the parent under pressure.
    ///
    /// Private to the transient (so concurrent `readBigAt` calls don't race
    /// with each other or with the parent's `next()`/`seek()`):
    ///   - `position`, `live_buffer`, `prefetch_handle`, `prefetch_range`,
    ///     `pre_acquired_slot`,
    ///     `stats`, `creator_query_id`, `reader_executor_log` ptr.
    ///
    /// Intentionally NOT propagated (the transient runs without them):
    ///   - `prefetch_pool`: a one-shot read can't amortise prefetch latency;
    ///     sharing the parent's pool would let a transient steal slots from a
    ///     concurrently-running sequential reader.
    ///   - `reader_executor_log`: the transient's stats land in global
    ///     `ProfileEvents` via the destructor anyway; we don't want a row per
    ///     `readBigAt` in `system.reader_executor_log`.
    auto t = std::make_unique<ReaderExecutor>(
        source, stored_objects, caches,
        window_size, min_bytes_for_seek, log_file_path);

    t->buffer_limit = buffer_limit;

#if USE_SSL
    t->decryption_layers = decryption_layers;
    t->decryption_headers = decryption_headers;
    t->decryption_initialized = decryption_initialized;
#endif
    t->data_start_offset = data_start_offset;
    t->seek(start_position);
    return t;
}

}
