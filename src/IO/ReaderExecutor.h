#pragma once

#include <IO/OffsetMap.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/ChainedBuffers.h>
#include <IO/ReadContinuityTracker.h>
#include <IO/LongConnectionLimit.h>
#include <IO/ICacheProvider.h>

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>
#include <Core/Defines.h>

#include <array>
#include <functional>
#include <memory>
#include <optional>

#include "config.h"
#if USE_SSL
#include <IO/ReaderExecutorDecryptor.h>
#endif

namespace DB
{

class ReadBufferFromFileBase;
class EncryptionHeaderCache;

/// Ordered cache chain, front = fastest tier.
using CacheChain = VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>;

/// Maps a logical read position to a `StoredObject` (via `OffsetMap`) and serves bytes from an
/// `IFileBasedSourceReader` as a `ChainedBuffers`, one block at a time. Drives the experimental
/// `use_reader_executor` read path. One instance per column-stream; not thread-safe.
class ReaderExecutor
{
public:
    /// Tunables the caller fills from settings. A null `long_connection_limit` disables connection
    /// reuse. An empty `cache_chain` disables caching. A null `encryption_header_cache` disables the
    /// header cache. Defaults live in `Core/Defines.h`, shared with the `reader_executor_*` settings.
    struct Options
    {
        size_t window_size = DEFAULT_READER_EXECUTOR_WINDOW_SIZE;
        size_t min_bytes_for_seek = DEFAULT_READER_EXECUTOR_MIN_BYTES_FOR_SEEK;
        size_t block_size = DEFAULT_READER_EXECUTOR_BLOCK_SIZE;
        size_t max_tail_for_drain = DEFAULT_READER_EXECUTOR_MAX_TAIL_FOR_DRAIN;
        std::shared_ptr<LongConnectionLimit> long_connection_limit = nullptr;
        std::shared_ptr<EncryptionHeaderCache> encryption_header_cache = nullptr;
        CacheChain cache_chain = {};
    };

    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects,
        Options options);

    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects);

    ~ReaderExecutor();

    /// Read the next block (<= `block_size`) and advance the position by the bytes read.
    /// An empty `ChainedBuffers` is EOF.
    ChainedBuffers readNextWindow();

    void seek(size_t new_position);

    /// Bound reads to logical offsets below `bound`; `nullopt` reads to the file end.
    void setReadUntil(std::optional<size_t> bound) { read_until = bound; }

    size_t getPosition() const { return position; }

    /// Logical file size (physical size minus the encryption headers), saturating to 0.
    size_t totalSize() const;
    bool hasUnknownSize() const { return offset_map.hasUnknownSize(); }

    String getFileName() const { return log_file_path; }

    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    /// Add a decryption layer (callable multiple times for layered encryption); no-op without SSL.
    void addDecryptionLayer(String path, KeyFinderFunc key_finder);

    /// Read the encryption headers and resolve keys. Must run before any read; no-op without layers.
    void initDecryption();

private:
    /// Per-instance read-path counters. `add` is the only mutator. It is also the single place a
    /// counter maps to its `ProfileEvent` and its modeled-cost contribution.
    struct Stats
    {
        enum Counter : size_t
        {
            SourceRequests,         /// chunks opened and read from the source
            BytesFromSource,        /// physical bytes read from the source
            DeliveredBytes,         /// useful bytes delivered to the caller (KPI denominator)
            IncompleteConnections,
            CacheGetRequests,
            CachePopulateRequests,
            WorkMicroseconds,
            DecryptMicroseconds,
            LongConnectionOpened,
            LongConnectionHits,
            LongConnectionFallbacks,
            LongConnectionBytes,
            NumCounters,
        };

        void add(Counter c, UInt64 value = 1);
        UInt64 get(Counter c) const { return values[c]; }

    private:
        std::array<UInt64, NumCounters> values{};
    };

    /// RAII timer: on scope exit adds its lifetime to a `Stats` timing counter.
    class StatTimer
    {
    public:
        StatTimer(Stats & target_, Stats::Counter counter_) : target(target_), counter(counter_) {}
        ~StatTimer() { target.add(counter, watch.elapsedMicroseconds()); }

        StatTimer(const StatTimer &) = delete;
        StatTimer & operator=(const StatTimer &) = delete;

    private:
        Stats & target;
        Stats::Counter counter;
        Stopwatch watch;
    };

    /// A held source connection (a bounded GET) reused across sequential windows. Offsets are
    /// object-local.
    struct LongConnection
    {
        std::unique_ptr<ReadBufferFromFileBase> buffer;
        String object_path;
        size_t opened_at = 0;
        size_t current_position = 0;
        size_t read_until = 0;
        LongConnectionSlot slot;

        bool servesObject(const String & path) const { return object_path == path; }
        bool atBound() const { return current_position >= read_until; }
        bool isComplete(bool at_eof) const { return at_eof || atBound(); }
        bool consumedAnyBytes() const { return current_position > opened_at; }
        /// Forward, within `bridgeable_gap`, and still below the bound. A window crossing the bound
        /// is served short (up to `read_until`), not rejected.
        bool canServeAt(size_t off, size_t bridgeable_gap) const
        {
            return off >= current_position && off - current_position <= bridgeable_gap && off < read_until;
        }

        size_t readInto(char * dst, size_t want);
        size_t skipForward(size_t gap, size_t block_bytes);

        struct DrainResult
        {
            size_t bytes = 0;
            bool failed = false;
        };
        /// Read out a tail <= `max_tail` so the connection completes (pool-reusable). Best-effort:
        /// a read error is caught and reported via `DrainResult::failed`, never thrown.
        DrainResult drainTail(size_t max_tail, size_t block_bytes, LoggerPtr log) noexcept;
    };

    /// EOF is `position >= totalSize` for a known size, or a latched `reached_eof` for an unknown size
    /// (a backward `seek` clears it). A `read_until` bound caps it earlier.
    bool atEnd() const
    {
        if (reached_eof || (read_until && position >= *read_until))
            return true;
        return !offset_map.hasUnknownSize() && position >= totalSize();
    }

    size_t clampReach(size_t predicted_end, size_t phys_pos) const;
    bool shouldOpenLongConnection() const;
    bool tryOpenLongConnection(const StoredObject & object, size_t object_offset);
    size_t readOneShot(const StoredObject & object, size_t object_offset, size_t want, char * dst);
    ChainedBuffers readObjectSlice(const StoredObject & object, size_t object_offset, size_t want, size_t file_base);
    /// The single source-read entry point; spans object boundaries via `OffsetMap::map`. A
    /// known-size short read is truncation and throws.
    ChainedBuffers readSource(size_t file_offset, size_t want);
    /// Serve the window through the cache chain: serve the cached prefix, then claim and fetch the
    /// miss ranges and populate them. A range another thread is already downloading is fetched
    /// through from source. Precondition: `!cache_chain.empty()`.
    ChainedBuffers readThroughCaches(size_t window_offset, size_t max_serve);
    void dropLongConnection();

    /// The only logical<->physical converters: physical = header-inclusive file coords; logical =
    /// payload coords. A raw `+/- data_start_offset` anywhere else is a bug.
    size_t toPhysical(size_t logical) const { return logical + data_start_offset; }
    size_t toLogical(size_t physical) const { chassert(physical >= data_start_offset); return physical - data_start_offset; }

    bool needsDecryption() const { return data_start_offset > 0; }
    void decryptInPlaceIfNeeded(char * data, size_t size, size_t logical_offset);
    /// Plaintext copy of `cipher`, decrypting each node at its logical offset. Nodes may alias cache
    /// buffers, so never decrypt through them; the plaintext path returns `cipher` untouched.
    ChainedBuffers decryptWindow(ChainedBuffers && cipher);

    std::shared_ptr<IFileBasedSourceReader> source;
    OffsetMap offset_map;
    String log_file_path;
    size_t window_size;
    size_t block_size;
    size_t position = 0;
    bool reached_eof = false;
    /// Hard upper bound on the logical read position; `nullopt` = read to end.
    std::optional<size_t> read_until;

    std::optional<LongConnection> long_conn;
    ReadContinuityTracker fetch_tracker;
    std::shared_ptr<LongConnectionLimit> long_connection_limit;
    std::shared_ptr<EncryptionHeaderCache> encryption_header_cache;
    CacheChain cache_chain;
    size_t min_bytes_for_seek;
    size_t max_tail_for_drain;

#if USE_SSL
    /// Immutable per-layer decryption config, parsed once by `initDecryption`. SSL builds only.
    ReaderExecutorDecryptor decryptor;
#endif
    /// Byte offset of the first plaintext byte in the physical stream (0 without encryption).
    size_t data_start_offset = 0;

    Stats stats;
    CurrentMetrics::Increment active_metric;

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
