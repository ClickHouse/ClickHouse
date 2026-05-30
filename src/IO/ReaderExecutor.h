#pragma once

#include <IO/Rope.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/ISourceReader.h>
#include <IO/SourceBufferLimit.h>

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <vector>

#include "config.h"
#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#endif

namespace DB
{

class PrefetchThreadPool;
class ReaderExecutorLog;
class PrefetchHandle;
class FilesystemReadPrefetchesLog;
enum class FilesystemPrefetchState : uint8_t;

/// Reads a logical file (one or more `StoredObject`s mapped by `OffsetMap`)
/// through a fastest-first cache chain, falling back to the source. Tuned for
/// sequential scans: keeps one source connection alive across windows
/// (`live_buffer`), reads one window ahead on a `PrefetchThreadPool`, and
/// shrinks its window/block sizes under memory pressure. Owns its cache and
/// decryption layers internally, so it is NOT wrapped by the legacy
/// async/decrypt/cache read buffers. One instance per column-stream; not
/// thread-safe beyond the documented prefetch-worker handoff (the prefetch
/// future's `get()` happens-before edge).
class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_WINDOW_SIZE = 8 * 1024 * 1024; /// 8 MiB
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 8 * 1024 * 1024; /// 8 MiB
    static constexpr size_t ROPE_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB per Rope node

    ReaderExecutor(
        std::shared_ptr<ISourceReader> source,
        const StoredObjects & objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches,
        size_t window_size = DEFAULT_WINDOW_SIZE,
        size_t min_bytes_for_seek = DEFAULT_MIN_BYTES_FOR_SEEK,
        size_t block_size = ROPE_BLOCK_SIZE,
        String log_file_path = {});

    /// Destructor must be out-of-line because LiveBuffer holds unique_ptr<ReadBufferFromFileBase>.
    ~ReaderExecutor();

    /// Returns an empty Rope at EOF.
    Rope readNextWindow();

    /// Seek to a new position. Discards any prefetched data.
    void seek(size_t new_position);

    /// A fresh executor starting at `start_position` that shares immutable state
    /// (caches, source, objects, decryption) but owns its own position / live
    /// buffer. Drives `PipelineReadBuffer::readBigAt` as a one-shot read. Shares
    /// `buffer_limit` (its connection counts against the server budget) but gets
    /// no `prefetch_pool`/log: a one-shot read can't amortise prefetch and would
    /// steal slots from a sequential reader.
    std::unique_ptr<ReaderExecutor> makeTransientForReadAt(size_t start_position) const;

    /// Whether `makeTransientForReadAt` / `readBigAt` is allowed. All current
    /// `ISourceReader` implementations support concurrent `open()` (each call
    /// returns an independent buffer), so this is true whenever a source is
    /// configured. Kept as a method so future non-reusable sources can opt out.
    bool canReadAt() const { return static_cast<bool>(source); }

    void setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool);
    void setBufferLimit(std::shared_ptr<SourceBufferLimit> limit);
    void setReaderExecutorLog(std::shared_ptr<ReaderExecutorLog> log_);
    /// Sink for `system.filesystem_read_prefetches_log`. When set, each
    /// resolved prefetch emits a row (USED on consume, CANCELLED_WITH_SEEK on
    /// seek-away) — preserving the legacy `AsynchronousBoundedReadBuffer`
    /// prefetch-logging contract under the executor.
    void setFilesystemReadPrefetchesLog(std::shared_ptr<FilesystemReadPrefetchesLog> log_);

    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    /// Add a decryption layer. Can be called multiple times for layered encryption.
    /// No-op in builds without SSL. Call initDecryption() once after all layers
    /// have been added to read and parse the on-disk headers.
    void addDecryptionLayer(String path, size_t buffer_size, KeyFinderFunc key_finder);

    /// Read the encryption headers (one per layer) and resolve keys via the
    /// configured key_finders. Must be called after addDecryptionLayer setup
    /// and before any read. No-op when no layers are configured or when
    /// ClickHouse is built without SSL.
    void initDecryption();

    size_t getPosition() const { return position; }

    size_t getSourceRequestsCount() const { return stats.source_requests; }
    /// Test-only: is there a prefetch currently scheduled for the next window?
    bool hasInflightPrefetch() const { return prefetch_handle != nullptr; }
    /// Test-only: number of cancelled prefetch handles still awaiting the
    /// destructor's drain (stashed on cancel so the worker can finish
    /// attaching before this executor's state is freed).
    size_t abandonedPrefetchCount() const { return abandoned_prefetches.size(); }

    /// Logical file size (physical size minus encryption headers).
    /// Saturates to 0 if the underlying objects sum to fewer bytes than the
    /// declared encryption headers — that file is corrupt/truncated; the
    /// next read (or initDecryption) will surface CANNOT_READ_ALL_DATA.
    size_t totalSize() const
    {
        size_t physical = offset_map.totalSize();
        return physical > data_start_offset ? physical - data_start_offset : 0;
    }

    /// True iff the underlying object had `StoredObject::UnknownSize` (e.g. S3
    /// HEAD without Content-Length). Callers that need to convert
    /// `totalSize()` into an `optional<size_t> file_size` MUST consult this
    /// first — `totalSize()` returns `~uint64_t::max()` in that case, which
    /// is meaningless as a literal byte count.
    bool hasUnknownSize() const { return offset_map.hasUnknownSize(); }

    /// Merge close-together ranges to reduce source request count.
    /// Ranges separated by less than min_gap are combined.
    static VectorWithMemoryTracking<ByteRange> mergeRanges(const VectorWithMemoryTracking<ByteRange> & ranges, size_t min_gap);

private:
    /// Read a specific physical range through the cache chain and source.
    /// `from_prefetch` is true when called from a prefetch worker; it routes
    /// cache-populate bytes to the sync vs. async counter.
    Rope readPhysicalWindow(ByteRange physical_window, bool from_prefetch);

    /// Read from source into the pre-allocated `blocks`. Tries live buffer first, falls back to stateless.
    /// `blocks` is consumed: blocks that receive data become RopeNodes in the returned Rope;
    /// blocks that receive no data (e.g., file ended early) are released when this function returns.
    Rope readFromSource(
        const StoredObject & object, size_t offset,
        VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset);

    /// Uses `set()` + `next()` so data lands directly in block memory instead
    /// of being copied through the live buffer's internal working buffer.
    Rope readFromLiveBufferIntoRope(
        VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> blocks, size_t logical_offset);

    /// Allocate enough OwnedRopeBuffers to cover `size` bytes, each ≤ `block_size`.
    /// `splits` (sorted, relative offsets within `[0, size)`) forces a block boundary at each
    /// listed offset so the resulting `OwnedRopeBuffer` allocations don't straddle those points.
    /// Used to keep user-window bytes and over-read bytes in separate buffers so each can be
    /// released independently.
    static VectorWithMemoryTracking<std::shared_ptr<OwnedRopeBuffer>> allocateBlocks(
        size_t size, size_t block_size, const VectorWithMemoryTracking<size_t> & splits = {});

    void maybeTriggerPrefetch();
    /// `reason` is the prefetch-log state recorded for the in-flight prefetch
    /// being dropped: `CANCELLED_WITH_SEEK` from `seek`, `UNNEEDED` from the
    /// destructor (the latter is never logged, matching the legacy path).
    void discardPrefetch(FilesystemPrefetchState reason);

    /// Append one row to `system.filesystem_read_prefetches_log` for the
    /// in-flight prefetch (described by `prefetch_range` / `prefetch_submit_time`
    /// / `prefetch_execution_watch`). No-op without a sink or for `UNNEEDED`.
    void emitPrefetchLog(FilesystemPrefetchState state, Int64 size);

    void drainAbandonedPrefetches(bool wait_finished = false);

    /// EOF detection has two cases:
    ///   - size known: `position >= totalSize()`.
    ///   - size unknown: the source's short return latches `reached_eof`.
    /// Seek backward clears `reached_eof` so the source can re-deliver.
    bool atEnd() const
    {
        return reached_eof || (!offset_map.hasUnknownSize() && position >= totalSize());
    }

    /// Try to acquire a buffer_limit slot up front so the next source read can
    /// be promoted to live without re-checking. When a slot is held (either
    /// pre-acquired or via an existing live_buffer), the read window shrinks
    /// to one `block_size` block because the live buffer streams a block at a
    /// time and allocating a larger rope just inflates the in-flight memory.
    void ensurePreAcquiredSlot();

    /// Acquire a `buffer_limit` slot for `object` and record the outcome in the
    /// `ReaderExecutorBufferSlot{Acquired,Failed}` counters. Shared by the
    /// pre-acquire path and the in-`readFromSource` fallback so both account
    /// every `SourceBufferLimit` attempt. Caller must hold a non-null buffer_limit.
    std::optional<SourceBufferSlot> acquireSlotCounted(const StoredObject & object);

    /// Effective window size for the next read: `effectiveBlockSize()` when we're
    /// (or about to be) on the live path, otherwise the constructor-supplied
    /// `window_size` clamped down by the current `MemoryPressureMonitor` level.
    /// Caller still caps by remaining file bytes.
    size_t effectiveWindowSize() const;

    /// Effective per-block allocation size: the configured `block_size` at
    /// normal memory, shrinks under pressure (see `MemoryPressureMonitor`). Used for both the
    /// `allocateBlocks` source-read tile and `decryptRope`'s output blocks so
    /// the in-flight allocation per call falls when free memory does.
    size_t effectiveBlockSize() const;

    /// Decrypt rope data; returns the input unchanged when no decryption layers
    /// are configured (which is always the case in builds without SSL).
    /// `const` because decryption_layers/decryption_headers are immutable after
    /// `initDecryption`; thread-safe for parallel calls (each creates its own
    /// Encryptor instance).
    Rope decryptRope(Rope rope, size_t logical_offset) const;

    std::shared_ptr<ISourceReader> source;
    StoredObjects stored_objects;  /// retained for makeTransientForReadAt
    OffsetMap offset_map;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    /// File path used only for `system.reader_executor_log` /
    /// `ReaderExecutorLogElement::source_file_path`. Cache identity is
    /// derived per-object by the cache providers themselves and no longer
    /// goes through the executor.
    String log_file_path;
    size_t window_size;
    size_t min_bytes_for_seek;
    size_t block_size;
    size_t position = 0;

    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    /// Single source of truth for "is there a prefetch scheduled":
    /// `prefetch_handle != nullptr`. `prefetch_range` is only meaningful when
    /// the handle is non-null.
    std::unique_ptr<PrefetchHandle> prefetch_handle;
    ByteRange prefetch_range;
    /// Prefetch-log metadata for the in-flight prefetch. `submit_time` is set
    /// at submit; `execution_watch` is set by the worker around its
    /// `readPhysicalWindow` (read on the foreground thread after `get()`, whose
    /// happens-before edge makes the cross-thread handoff safe — same model as
    /// `stats`). Reset at submit so a never-run (cancelled) prefetch logs no
    /// execution timing. Only meaningful when `prefetch_handle != nullptr`.
    std::chrono::system_clock::time_point prefetch_submit_time;
    std::optional<Stopwatch> prefetch_execution_watch;
    /// Cancelled prefetches whose worker may still be inside the pool job
    /// slot. The destructor waits on each; running calls sweep finished ones
    /// to keep the vector bounded under seek-heavy workloads.
    VectorWithMemoryTracking<std::unique_ptr<PrefetchHandle>> abandoned_prefetches;
    /// Set when the source returned fewer bytes than requested AND the
    /// total file size is unknown — in that mode the short return IS the
    /// EOF marker. `readNextWindow` consults this so a subsequent call
    /// short-circuits to EOF without re-issuing a read.
    bool reached_eof = false;

    /// Live buffer: keeps a connection open for sequential reads. The
    /// object path lives inside `slot.objectPath()` (one source of truth).
    struct LiveBuffer
    {
        size_t current_position = 0;
        std::unique_ptr<ReadBufferFromFileBase> buffer;
        SourceBufferSlot slot;
    };

    std::optional<LiveBuffer> live_buffer;
    /// While streaming sequentially through a DiskCache/FileCache segment, hold
    /// a bare ref to that segment so a mid-read eviction can't snap the next
    /// miss head back to the segment start and force a connection reset + a
    /// re-read of bytes already delivered. Re-pointed each window to the
    /// segment under the live frontier; dropped on seek/EOF/connection reset.
    ICacheHandle::CacheSegmentPin inflight_segment_pin;
    std::shared_ptr<SourceBufferLimit> buffer_limit;
    std::shared_ptr<ReaderExecutorLog> reader_executor_log;
    std::shared_ptr<FilesystemReadPrefetchesLog> prefetches_log;
    String creator_query_id;
    /// Stable per-executor id for the `reader_id` column of
    /// `system.filesystem_read_prefetches_log` (random 8-char, like the legacy
    /// per-buffer id). Empty until the prefetches-log sink is attached.
    String prefetch_reader_id;

    /// Slot reserved at the top of `readNextWindow` so the next source read is
    /// promoted to live without re-checking; consumed into `live_buffer` on
    /// that read (released here if the open fails). Reserved-for-path is
    /// `pre_acquired_slot->objectPath()`.
    std::optional<SourceBufferSlot> pre_acquired_slot;

    /// Drop `pre_acquired_slot` when it was reserved for an object other than
    /// `target_path`. Must run before a `readFromSource` / `seek` into a
    /// different object, else the stale slot leaks while `readFromSource`'s
    /// fallback acquires a second one — halving `max_remote_read_connections`.
    void releaseStalePreAcquiredSlot(const String & target_path);

#if USE_SSL
    /// Decryption
    struct DecryptionLayer
    {
        String path;
        size_t buffer_size;
        KeyFinderFunc key_finder;
        /// Populated by initDecryption
        String key;
    };

    VectorWithMemoryTracking<DecryptionLayer> decryption_layers;
    VectorWithMemoryTracking<FileEncryption::Header> decryption_headers;
    bool decryption_initialized = false;
#endif
    size_t data_start_offset = 0;  /// N * Header::kSize (0 when no encryption)

    /// Per-executor accumulating stats. Flushed to ProfileEvents and logged at
    /// destruction. Cumulative; the destructor emits one summary line so
    /// triaging a slow query needs only the server log, not a separate trace.
    struct Stats
    {
        /// Bytes delivered to the consumer, split by where they came from.
        /// Attributed by the serving provider's `tier()`.
        size_t bytes_from_page_cache = 0;
        size_t bytes_from_filesystem_cache = 0;
        size_t bytes_from_source = 0;
        /// Bytes written back into cache layers via `put`, split by the
        /// context that ran the populating window: a foreground (synchronous)
        /// read vs. a background prefetch worker.
        size_t bytes_pushed_to_cache_sync = 0;
        size_t bytes_pushed_to_cache_async = 0;
        size_t cache_get_requests = 0;
        size_t cache_populate_requests = 0;
        size_t source_requests = 0;
        UInt64 cache_get_us = 0;
        UInt64 cache_populate_us = 0;
        UInt64 source_read_us = 0;
        UInt64 decrypt_us = 0;
        UInt64 prefetch_wait_us = 0;
        UInt64 sync_read_us = 0;
        size_t prefetch_hits = 0;
        size_t prefetch_cancelled = 0;
        size_t prefetch_pool_full = 0;
        /// Discarding a running prefetch: `discardPrefetch` blocked on
        /// `get()` because `tryCancel` lost the race against the worker.
        /// All work the worker did is thrown away.
        size_t prefetch_discarded_running = 0;
        UInt64 prefetch_discard_wait_us = 0;
        /// Bytes a running prefetch materialised into a rope that was then
        /// discarded (consumer seeked/closed away before consuming it). The
        /// cache `put` that ran in the same window is NOT counted here — those
        /// bytes persist in cache for a later read.
        size_t prefetch_wasted_bytes = 0;
    };
    /// `mutable` so the `const` `decryptRope` can accumulate timings. Stats are
    /// observability, not state; worker/foreground writes are serialized by the
    /// prefetch future's `get()` happens-before edge.
    mutable Stats stats;

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
