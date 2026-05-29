#pragma once

#include <IO/Rope.h>
#include <IO/OffsetMap.h>
#include <IO/ICacheProvider.h>
#include <IO/ISourceReader.h>
#include <IO/SourceBufferLimit.h>

#include <Common/Logger.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>
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
        String log_file_path = {});

    /// Destructor must be out-of-line because LiveBuffer holds unique_ptr<ReadBufferFromFileBase>.
    ~ReaderExecutor();

    /// Returns an empty Rope at EOF.
    Rope readNextWindow();

    /// Seek to a new position. Discards any prefetched data.
    void seek(size_t new_position);

    /// Build a transient ReaderExecutor configured to start at `start_position`,
    /// sharing immutable state (caches, source, objects, cache_key, decryption)
    /// but owning its own mutable state (position, live_buffer, prefetch).
    /// Used by `PipelineReadBuffer::readBigAt` to drive a one-shot read via the
    /// regular pipeline without disturbing this executor or duplicating the
    /// cache-walk / source-read logic.
    ///
    /// The transient deliberately uses neither `prefetch_pool` nor
    /// `buffer_limit` — those exist to coordinate state across calls on the
    /// same stream and would compete with the main executor for resources.
    std::unique_ptr<ReaderExecutor> makeTransientForReadAt(size_t start_position) const;

    /// Whether `makeTransientForReadAt` / `readBigAt` is allowed. All current
    /// `ISourceReader` implementations support concurrent `open()` (each call
    /// returns an independent buffer), so this is true whenever a source is
    /// configured. Kept as a method so future non-reusable sources can opt out.
    bool canReadAt() const { return static_cast<bool>(source); }

    void setPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool);
    void setBufferLimit(std::shared_ptr<SourceBufferLimit> limit);
    void setReaderExecutorLog(std::shared_ptr<ReaderExecutorLog> log_);

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
    Rope readPhysicalWindow(ByteRange physical_window);

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
    void discardPrefetch();

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
    /// to ROPE_BLOCK_SIZE because the live buffer streams 1 MiB at a time and
    /// allocating a larger rope just inflates the in-flight memory.
    void ensurePreAcquiredSlot();

    /// Effective window size for the next read: `effectiveBlockSize()` when we're
    /// (or about to be) on the live path, otherwise the constructor-supplied
    /// `window_size` clamped down by the current `MemoryPressureMonitor` level.
    /// Caller still caps by remaining file bytes.
    size_t effectiveWindowSize() const;

    /// Effective per-block allocation size: `ROPE_BLOCK_SIZE` at normal memory,
    /// shrinks under pressure (see `MemoryPressureMonitor`). Used for both the
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
    size_t position = 0;

    std::shared_ptr<PrefetchThreadPool> prefetch_pool;
    /// Single source of truth for "is there a prefetch scheduled":
    /// `prefetch_handle != nullptr`. `prefetch_range` is only meaningful when
    /// the handle is non-null.
    std::unique_ptr<PrefetchHandle> prefetch_handle;
    ByteRange prefetch_range;
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
    std::shared_ptr<SourceBufferLimit> buffer_limit;
    std::shared_ptr<ReaderExecutorLog> reader_executor_log;
    String creator_query_id;

    /// Slot pre-acquired at the top of readNextWindow. When present, the
    /// next source read is guaranteed to be promoted to live (no re-attempt
    /// in readFromSource), and we use a 1 MiB read window because the live
    /// buffer streams 1 MiB at a time anyway. The slot is consumed (moved
    /// into live_buffer) on the first source read of this window; if the
    /// open fails it's released here. The reserved-for-path lives in
    /// `pre_acquired_slot->objectPath()`.
    std::optional<SourceBufferSlot> pre_acquired_slot;

    /// Drop `pre_acquired_slot` if it was reserved for a different object
    /// than `target_path`. No-op when no slot is held or the path already
    /// matches. Call before `readFromSource` for a known object or before
    /// `seek` lands in a different object — otherwise the stale slot stays
    /// held until executor destruction while `readFromSource`'s fallback
    /// quietly acquires a SECOND slot, halving effective
    /// `max_remote_read_connections` capacity.
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
        size_t cache_hit_bytes = 0;
        size_t cache_miss_bytes = 0;
        size_t cache_populated_bytes = 0;
        size_t allocated_bytes = 0;
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
        size_t prefetch_discarded_bytes = 0;
    };
    /// `mutable` so the `const` `decryptRope` can accumulate `decrypt_us`.
    /// `decryptRope`'s `const` documents thread-safety for parallel calls
    /// from prefetch workers (decryption_layers/decryption_headers stay
    /// immutable post-init); stats are observability, not state, and
    /// writes from worker + foreground are serialized via the prefetch
    /// future's `get()` happens-before edge (same model as `source_read_us`).
    mutable Stats stats;

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
