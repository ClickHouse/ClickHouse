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
    extern const Event ReaderExecutorPrefetchDiscardedRunning;
    extern const Event ReaderExecutorPrefetchDiscardWaitMicroseconds;
    extern const Event ReaderExecutorPrefetchDiscardedBytes;
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
    creator_query_id = String(CurrentThread::getQueryId());
    LOG_DEBUG(log, "Created: {} objects, total_size={}, window_size={}, min_bytes_for_seek={}, {} caches",
        objects.size(), offset_map.totalSize(), window_size, min_bytes_for_seek, caches.size());
}

ReaderExecutor::~ReaderExecutor()
{
    discardPrefetch();
    drainAbandonedPrefetches(/*wait_finished=*/true);
    CurrentMetrics::sub(CurrentMetrics::ReaderExecutorActive);

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
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardedRunning, stats.prefetch_discarded_running);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardWaitMicroseconds, stats.prefetch_discard_wait_us);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorPrefetchDiscardedBytes, stats.prefetch_discarded_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadBytes, stats.over_read_bytes);
    ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadServedBytes, stats.over_read_served_bytes);

    LOG_DEBUG(log,
        "Destroyed: cache_hit_bytes={} miss_bytes={} populated={} allocated={} "
        "get_reqs={} populate_reqs={} src_reqs={} "
        "get_us={} populate_us={} src_us={} decrypt_us={} "
        "prefetch_wait_us={} sync_read_us={} "
        "prefetch_hits={} prefetch_cancelled={} prefetch_pool_full={} "
        "prefetch_discarded_running={} prefetch_discard_wait_us={} prefetch_discarded_bytes={} "
        "over_read={} over_read_served={}",
        stats.cache_hit_bytes, stats.cache_miss_bytes,
        stats.cache_populated_bytes, stats.allocated_bytes,
        stats.cache_get_requests, stats.cache_populate_requests, stats.source_requests,
        stats.cache_get_us, stats.cache_populate_us,
        stats.source_read_us, stats.decrypt_us,
        stats.prefetch_wait_us, stats.sync_read_us,
        stats.prefetch_hits, stats.prefetch_cancelled, stats.prefetch_pool_full,
        stats.prefetch_discarded_running, stats.prefetch_discard_wait_us, stats.prefetch_discarded_bytes,
        stats.over_read_bytes, stats.over_read_served_bytes);

    if (reader_executor_log)
    {
        ReaderExecutorLogElement elem;
        elem.event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        elem.query_id = creator_query_id;
        elem.source_file_path = log_file_path;
        /// Logical (user-visible) bytes — `totalSize()` subtracts
        /// `data_start_offset` for encrypted reads so the value lines up
        /// with `cache_hit_bytes` / `cache_miss_bytes`. `nullopt` when the
        /// underlying object had `StoredObject::UnknownSize`.
        elem.total_size = offset_map.hasUnknownSize()
            ? std::optional<UInt64>{}
            : std::optional<UInt64>{totalSize()};
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
        elem.prefetch_discarded_running = stats.prefetch_discarded_running;
        elem.prefetch_discard_wait_us = stats.prefetch_discard_wait_us;
        elem.prefetch_discarded_bytes = stats.prefetch_discarded_bytes;
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
    /// On the live path the buffer streams one block at a time, so a wider
    /// window only inflates the in-flight rope; clamp to `effectiveBlockSize`.
    /// The stateless path keeps the full window to amortise HTTP setup.
    /// Both clamp against the memory-pressure ceiling.
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
    if (!prefetch_pool || prefetch_handle || atEnd())
        return;

    drainAbandonedPrefetches();

    size_t logical_size = totalSize();

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
    drainAbandonedPrefetches();

    auto local_handle = std::move(prefetch_handle);
    if (!local_handle)
        return;

    LOG_TRACE(log, "Prefetch: discarding [{}, {})", prefetch_range.offset, prefetch_range.end());

    /// `tryCancel` succeeded: the worker hadn't started yet. Stash the
    /// handle for the destructor to wait on — the worker, when it picks
    /// the task up, takes the cancellation-exception path and never
    /// touches this executor's mutable state.
    if (local_handle->tryCancel())
    {
        abandoned_prefetches.push_back(std::move(local_handle));
        return;
    }

    /// `tryCancel` lost the race — the worker is mid-`readPhysicalWindow`,
    /// mutating `live_buffer`, `over_read_buffer`, `reached_eof`, `stats`
    /// via the captured `this`. Block so the caller can safely overwrite
    /// that state on return. Everything the worker produced is wasted.
    ++stats.prefetch_discarded_running;
    StopwatchAccumulator wait_scope(stats.prefetch_discard_wait_us);
    try
    {
        auto rope = local_handle->get();
        stats.prefetch_discarded_bytes += rope.totalBytes();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Discarded prefetch task threw");
    }
}

void ReaderExecutor::drainAbandonedPrefetches(bool wait_finished)
{
    abandoned_prefetches.erase(
        std::remove_if(abandoned_prefetches.begin(), abandoned_prefetches.end(),
            [this, wait_finished](std::unique_ptr<PrefetchHandle> & h)
            {
                if (!wait_finished && !h->isFinished())
                    return false;
                try
                {
                    std::ignore = h->get();
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Abandoned prefetch task threw");
                }
                return true;
            }),
        abandoned_prefetches.end());
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

    Rope header_rope = readPhysicalWindow(ByteRange{0, data_start_offset});

    /// Under size-unknown sources `readPhysicalWindow` latches `reached_eof`
    /// on short returns instead of throwing, so an empty rope means
    /// "empty object" (same as the size-known empty branch above) and a
    /// partial rope means corrupted/truncated.
    if (offset_map.hasUnknownSize() && header_rope.totalBytes() == 0)
    {
        LOG_DEBUG(log, "initDecryption: unknown-size source returned 0 bytes (empty object), skipping");
        return;
    }
    if (header_rope.totalBytes() != data_start_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Encrypted source returned {} header bytes, expected {} (corrupted/truncated)",
            header_rope.totalBytes(), data_start_offset);

    /// Stacked encryption layout: only `h0` is plaintext; every later header
    /// is wrapped by all layers above it (`[h0, enc0(h1), enc0(enc1(h2)), ...]`).
    /// Parsing `hi` peels layers `0..i-1` at their current keystream offsets —
    /// same per-layer stepping as the payload path; see `decryptRope`.
    VectorWithMemoryTracking<FileEncryption::Encryptor> initialized_encryptors;
    initialized_encryptors.reserve(decryption_layers.size());
    size_t offset = 0;
    for (size_t i = 0; i < decryption_layers.size(); ++i)
    {
        auto & layer = decryption_layers[i];

        /// Copy the header's 64 bytes into a mutable buffer so we can
        /// decrypt in place across the already-initialized layers.
        std::array<char, FileEncryption::Header::kSize> hdr_bytes{};
        {
            Rope slice = header_rope.slice(ByteRange{offset, FileEncryption::Header::kSize});
            chassert(slice.totalBytes() == FileEncryption::Header::kSize);
            slice.copyTo(hdr_bytes.data(), ByteRange{offset, FileEncryption::Header::kSize});
        }

        for (size_t j = 0; j < initialized_encryptors.size(); ++j)
        {
            const size_t layer_keystream_offset = (i - 1 - j) * FileEncryption::Header::kSize;
            initialized_encryptors[j].setOffset(layer_keystream_offset);
            initialized_encryptors[j].decrypt(hdr_bytes.data(), hdr_bytes.size(), hdr_bytes.data());
        }

        ReadBufferFromMemory rb(hdr_bytes.data(), hdr_bytes.size());
        FileEncryption::Header header;
        header.read(rb);
        layer.key = layer.key_finder(header.key_fingerprint, layer.path);
        decryption_headers.push_back(std::move(header));

        /// Materialise this layer's encryptor for the next iteration to use.
        initialized_encryptors.emplace_back(
            decryption_headers.back().algorithm,
            layer.key,
            decryption_headers.back().init_vector);

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

    StopwatchAccumulator decrypt_scope(stats.decrypt_us);

    /// Stream block-by-block: decrypting the whole rope at once would hold
    /// every output buffer live simultaneously, doubling peak memory. CTR
    /// mode is position-addressable, so per-block `setOffset` gives the same
    /// keystream as a single-shot decrypt.
    ///
    /// Encryptors are constructed once and reused across blocks.
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

        /// Drain by peek/advance from the rope's front: the cursor walks the
        /// encrypted bytes in order, so the physical-vs-logical header offset
        /// (`data_start_offset`) never needs to be reasoned about here.
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

        /// Per-layer keystream offset: with `N` layers (0 = outermost,
        /// N-1 = innermost), layer `i`'s stream contains the inner layers'
        /// headers ahead of the payload, so its CTR offset for `user_offset`
        /// is `user_offset + (N - 1 - i) * Header::kSize`. The innermost
        /// layer uses `user_offset` directly. See
        /// `ReadBufferFromEncryptedFile::nextImpl` for the same formula
        /// expressed across nested buffers.
        for (size_t i = 0; i < encryptors.size(); ++i)
        {
            const size_t layer_keystream_offset = logical_offset + pos
                + (encryptors.size() - 1 - i) * FileEncryption::Header::kSize;
            encryptors[i].setOffset(layer_keystream_offset);
            encryptors[i].decrypt(buf->data(), chunk, buf->data());
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
        /// Consume the prefetched rope BEFORE applying the EOF gate: an
        /// unknown-size prefetch worker may latch `reached_eof` while
        /// still returning the file's final bytes — gating on `atEnd()`
        /// first would `discardPrefetch` and drop those bytes.
        /// Move-before-touch: same future-consumption contract as
        /// `discardPrefetch`.
        auto local_handle = std::move(prefetch_handle);

        if (local_handle->tryCancel())
        {
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
            /// If a seek landed inside the prefetched window, trim the prefix
            /// below so `rope.range().offset` matches `position`.
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
        if (atEnd())
        {
            LOG_TRACE(log, "readNextWindow: EOF at position {}", position);
            /// Release per-stream resources at EOF instead of waiting for
            /// the caller to drop the `PipelineReadBuffer`. A subsequent
            /// seek-back will re-open and re-acquire.
            live_buffer.reset();
            pre_acquired_slot.reset();
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

    const size_t new_physical = new_position + data_start_offset;
    size_t new_obj_file_offset = 0;
    const StoredObject * new_obj = offset_map.findObjectAt(new_physical, &new_obj_file_offset);

    /// `pre_acquired_slot` is keyed to the old object. Drop it when the new
    /// position lands in a different (or no) object — pairs with the
    /// path-mismatch reset in `readFromSource`.
    releaseStalePreAcquiredSlot(new_obj ? new_obj->remote_path : String{});

    /// Reset `live_buffer` when the seek target no longer continues from it.
    /// `readFromSource` does the same check, but a cache-hit path skips that
    /// branch entirely — without resetting here the stale connection (and
    /// its `SourceBufferSlot`) would stay open until EOF or destruction,
    /// burning `max_remote_read_connections` capacity.
    if (live_buffer)
    {
        const bool live_continues = new_obj
            && live_buffer->slot.objectPath() == new_obj->remote_path
            && live_buffer->current_position == new_physical - new_obj_file_offset;
        if (!live_continues)
        {
            LOG_TRACE(log, "seek: live buffer for {} (at {}) no longer matches target, closing",
                live_buffer->slot.objectPath(), live_buffer->current_position);
            live_buffer.reset();
        }
    }

    /// `over_read_buffer` is paired with the live connection's position;
    /// even if `live_buffer` survives, the speculative bytes are scoped to
    /// the previous window.
    over_read_buffer = {};
    position = new_position;
    reached_eof = false;

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
        while (split_it != splits.end() && *split_it <= pos)
            ++split_it;

        const size_t boundary = (split_it != splits.end()) ? std::min(*split_it, size) : size;
        const size_t chunk = std::min(block_size, boundary - pos);
        blocks.push_back(std::make_shared<OwnedRopeBuffer>(chunk));
        pos += chunk;
    }
    return blocks;
}

/// Zero-copy set()+next() path when the buffer supports it. Asynchronous
/// readers (`pread_threadpool`, io_uring) read into their own allocation
/// assuming `memory.size() == internal_buffer.size()`, so `set()` would
/// corrupt the heap when `chunk` exceeds the buffer's constructor-time size —
/// for those, fall back to `read()`.
///
/// Returned value is `0` only when the source signals EOF. Short positive
/// `next` returns are looped so a partial fill never reaches the caller as
/// `actual < pr.size`.
static size_t readIntoBlock(ReadBuffer & buf, char * dest, size_t chunk)
{
    if (buf.supportsExternalBufferMode())
    {
        size_t total = 0;
        while (total < chunk)
        {
            /// Re-arm at `dest + total`: the source's internal position has
            /// advanced by `total` already, so successive `next` calls land
            /// contiguously in `dest`.
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

    return rope;
}

Rope ReaderExecutor::readFromSource(
    const StoredObject & object, size_t offset,
    VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset)
{
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

    if (live_buffer)
    {
        LOG_TRACE(log, "readFromSource: closing live buffer for {} (was at {}), need {}:{}",
            live_buffer->slot.objectPath(), live_buffer->current_position, object.remote_path, offset);
        live_buffer.reset();
    }

    /// `pre_acquired_slot` keyed to a different object: drop it now, otherwise
    /// the `buffer_limit` fallback below would acquire a SECOND slot and the
    /// stale one would stay held until executor destruction.
    releaseStalePreAcquiredSlot(object.remote_path);

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
    }

    /// No slot available — open a one-shot connection without storing it as
    /// `live_buffer`. Dropped when this function returns.
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

    return rope;
}

Rope ReaderExecutor::readPhysicalWindow(ByteRange physical_window)
{
    LOG_TRACE(log, "readPhysicalWindow [{}, {})", physical_window.offset, physical_window.end());

    Rope result;
    IntervalSet covered;   /// disjoint logical bytes already materialised in `result`
    /// Both vectors live to scope exit so `~ICacheHandle` (which does the
    /// deferred LRU bump) runs AFTER every `put` below — see `~DiskCacheHandle`.
    VectorWithMemoryTracking<std::unique_ptr<ICacheHandle>> miss_handles;
    VectorWithMemoryTracking<std::unique_ptr<ICacheHandle>> hit_only_handles;

    /// Over-read pre-check: bytes the previous source-read pulled past its
    /// window (advancing `live_buffer` through them) and we kept in
    /// `over_read_buffer`. Serving them here lets the live-buffer reuse path
    /// take over the rest of the window — the upcoming source-read starts at
    /// the exact byte where `live_buffer` is positioned.
    if (!over_read_buffer.empty())
    {
        /// Don't bump `cache_hit_bytes`: these bytes were already accounted
        /// as `cache_miss_bytes` in the source-read that produced them.
        Rope served = over_read_buffer.slice(physical_window);
        for (const auto & node : served.getNodes())
            covered.add(ByteRange{node.logical_offset, node.size});
        const size_t served_bytes = served.totalBytes();
        stats.over_read_served_bytes += served_bytes;
        LOG_TRACE(log, "readPhysicalWindow: over-read served {} bytes", served_bytes);
        result.append(std::move(served));

        /// Half-open `[physical_window.end(), +∞)`. `size_t::max()/2` avoids
        /// `offset + size` overflow inside `ByteRange::end()`.
        const size_t tail_size = std::numeric_limits<size_t>::max() / 2;
        over_read_buffer = over_read_buffer.slice(
            ByteRange{physical_window.end(), tail_size});
    }

    /// Walk the cache chain. Each cache reports hits/misses at its own
    /// granularity (which may extend past the requested range). Hits are
    /// appended only for not-yet-covered subranges so `result` stays disjoint
    /// even when a lower cache hit overlaps a higher cache hit. Misses
    /// propagate to the next cache.
    VectorWithMemoryTracking<ByteRange> remaining = covered.subtract(physical_window);
    for (auto & cache : caches)
    {
        VectorWithMemoryTracking<ByteRange> still_missing;

        for (const auto & r : remaining)
        {
            /// Split `r` by object boundaries so each `cache->lookup` carries
            /// a single `StoredObject` and the provider can derive a per-object
            /// key / origin. Handles still report ranges in file-level
            /// coordinates — the per-object translation lives inside the provider.
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

                    /// `hit` is the cache's segment range; `piece_range` is
                    /// the actual request. Clamp `get` to the intersection to
                    /// avoid allocating buffer memory for segment bytes outside
                    /// the request. Misses stay segment-sized so the next layer
                    /// (or the source) can fully populate this cache via `put`.
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

    for (const auto & fr : fetch_ranges)
    {
        auto physical_ranges = offset_map.map(fr);
        size_t logical_pos = fr.offset;

        for (const auto & pr : physical_ranges)
        {
            LOG_TRACE(log, "readPhysicalWindow: source read object={}, offset={}, size={}",
                pr.object.remote_path, pr.object_offset, pr.size);

            /// Split at the user-window edges so user-data and over-read bytes
            /// land in separate `OwnedRopeBuffer`s — when the caller drops the
            /// returned rope, the user-data buffer's refcount falls to zero
            /// even if `over_read_buffer` still holds the tail block.
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
            /// Size-known short reads are fatal (the map promised those bytes;
            /// silently shrinking would shift later logical offsets). Size-unknown
            /// short reads are the only way to learn EOF — latch `reached_eof`.
            if (actual != pr.size)
            {
                if (!offset_map.hasUnknownSize())
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "ReaderExecutor: short read from {} at offset {}: requested {} bytes, got {}",
                        pr.object.remote_path, pr.object_offset, pr.size, actual);
                reached_eof = true;
            }

            /// Use `actual` so the recorded range tracks what the source
            /// actually delivered — diverges from `pr.size` only at EOF.
            ByteRange pr_range{logical_pos, actual};
            auto uncovered = covered.subtract(pr_range);
            for (const auto & sub : uncovered)
            {
                result.append(source_rope.extract(sub));
                covered.add(sub);
            }

            /// Retain whatever the source delivered past `physical_window.end()`
            /// — the over-read pre-check on the next call serves these bytes
            /// regardless of `live_buffer` state. Critical for unknown-size
            /// EOF inside a cache-widened miss: bytes between
            /// `physical_window.end()` and the actual file end would otherwise
            /// be dropped before the caller could read them.
            /// A pre-window prefix is always dropped (sequential reads move
            /// forward; the over-read pre-check would discard it anyway).
            /// The overflow guard at the end of this function bounds total
            /// retention to `window_size`.
            Rope tail = source_rope.slice(
                ByteRange{physical_window.end(), std::numeric_limits<size_t>::max() / 2});
            if (!tail.empty())
            {
                stats.over_read_bytes += tail.totalBytes();
                over_read_buffer.append(std::move(tail));
            }

            logical_pos += pr.size;
        }
    }

    /// `result` is disjoint by construction (every append went through the
    /// `covered` guard), so each slice handed to `put` has at most one node
    /// per byte. The slice may be shorter than `miss.size` at EOF.
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

    /// Over-read past `window_size` means the caller is not consuming our
    /// lookahead — drop both the buffer and `live_buffer` (the next call
    /// will need bytes before the connection's position anyway).
    if (over_read_buffer.totalBytes() > window_size)
    {
        LOG_TRACE(log, "readPhysicalWindow: over-read buffer overflow ({} > {}), dropping it and live_buffer",
            over_read_buffer.totalBytes(), window_size);
        ProfileEvents::increment(ProfileEvents::ReaderExecutorOverReadOverflow);
        over_read_buffer = {};
        live_buffer.reset();
    }

    /// Release the slot if this window didn't open a connection — a fully
    /// warm-cache window would otherwise accumulate a phantom slot per
    /// executor, consuming `max_remote_read_connections` quota and showing
    /// up in `system.remote_read_connections`. (If the read promoted to live,
    /// `pre_acquired_slot` is already empty — moved into `live_buffer.slot`.)
    if (pre_acquired_slot && !live_buffer)
        pre_acquired_slot.reset();

    auto sliced = result.slice(physical_window);

    /// The returned bytes must form a single contiguous run from
    /// `physical_window.offset` — a hole would shift the caller's offset
    /// interpretation. Under EOF the run may end early but stays
    /// contiguous from the front.
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
    /// `buffer_limit` is shared so the transient's live connection counts
    /// against the server-wide budget. `prefetch_pool` and
    /// `reader_executor_log` are intentionally NOT propagated: a one-shot
    /// `readBigAt` can't amortise prefetch latency (and would steal slots
    /// from a concurrent sequential reader), and per-call log rows would
    /// spam `system.reader_executor_log`.
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
