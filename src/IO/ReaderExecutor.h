#pragma once

#include <IO/OffsetMap.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/ChainedBuffers.h>
#include <IO/ReadContinuityTracker.h>
#include <IO/LongConnectionLimit.h>

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <base/types.h>

#include <array>
#include <memory>
#include <optional>

namespace DB
{

class ReadBufferFromFileBase;

/// Maps a logical read position to a `StoredObject` (via `OffsetMap`) and serves
/// bytes from an `IFileBasedSourceReader` as a `ChainedBuffers`, one block at a time.
/// Drives the experimental `use_reader_executor` read path. One instance per
/// column-stream; not thread-safe.
class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB
    static constexpr size_t DEFAULT_MIN_BYTES_FOR_SEEK = 2 * 1024 * 1024; /// 2 MiB
    static constexpr size_t DEFAULT_MAX_TAIL_FOR_DRAIN = 1 * 1024 * 1024; /// 1 MiB

    /// Tunables, grouped so the constructor stays stable as the executor gains knobs (cache,
    /// prefetch, ...). `long_connection_limit` null disables connection reuse (the stateless path);
    /// the caller fills these from settings on the `use_reader_executor` path.
    struct Options
    {
        size_t min_bytes_for_seek = DEFAULT_MIN_BYTES_FOR_SEEK;
        size_t block_size = DEFAULT_BLOCK_SIZE;
        size_t max_tail_for_drain = DEFAULT_MAX_TAIL_FOR_DRAIN;
        std::shared_ptr<LongConnectionLimit> long_connection_limit = nullptr;
    };

    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects,
        Options options);

    /// All-defaults overload (cannot be a default argument: `Options{}` in a member declaration
    /// would need the initializers in a complete-class context).
    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects);

    ~ReaderExecutor();

    /// Read the next block (<= `block_size`, clamped to the current object's end for
    /// known-size objects) into a fresh chain buffer and advance the position by the bytes
    /// read. Returns a single-node `ChainedBuffers` at the current position; an empty `ChainedBuffers` is EOF.
    ChainedBuffers readNextWindow();

    void seek(size_t new_position);

    /// Bound reads to logical offsets below `position`; `nullopt` reads to the
    /// file end. Used by callers (e.g. `StorageLog`) that need a hard read bound.
    void setReadUntil(std::optional<size_t> bound) { read_until = bound; }

    size_t getPosition() const { return position; }

    size_t totalSize() const { return offset_map.totalSize(); }
    bool hasUnknownSize() const { return offset_map.hasUnknownSize(); }

    /// Front object's `remote_path`, used to name the source in diagnostics;
    /// empty when no objects are configured.
    String getFileName() const { return log_file_path; }

private:
    /// Per-instance read-path counters. `add` is the only mutator and the single place a
    /// counter maps to its ProfileEvent (and modeled-cost contribution), so they never
    /// drift and every update is instantly observable. The cache counters have no caller
    /// in this minimal slice, so they stay 0 until caching lands.
    struct Stats
    {
        enum Counter : size_t
        {
            SourceRequests,         /// chunks opened and read from the source
            BytesFromSource,        /// physical bytes read from the source
            RequestedBytes,         /// useful bytes delivered to the caller (KPI denominator)
            IncompleteConnections,
            CacheGetRequests,
            CachePopulateRequests,
            WorkMicroseconds,
            LongConnectionOpened,       /// held connections opened for reuse
            LongConnectionHits,         /// windows served from a held connection
            LongConnectionFallbacks,    /// opens skipped because no slot was free
            LongConnectionBytes,        /// bytes served through held connections
            NumCounters,
        };

        void add(Counter c, UInt64 value = 1);
        UInt64 get(Counter c) const { return values[c]; }

    private:
        std::array<UInt64, NumCounters> values{};
    };

    /// RAII timer: on scope exit adds its lifetime to a `Stats` timing counter via `add`.
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

    /// A held source connection (a bounded GET) reused across sequential windows:
    /// `readInto` streams forward from it, `skipForward` bridges a small forward gap by
    /// discarding bytes on the open stream. Offsets are object-local.
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
        /// Whether any bytes have been consumed from the stream (read or skipped) since it opened.
        bool consumedAnyBytes() const { return current_position > opened_at; }
        /// Forward, within `bridgeable_gap`, and `[off, off+want)` stays inside the bound.
        bool canContinue(size_t off, size_t want, size_t bridgeable_gap) const
        {
            return off >= current_position && off - current_position <= bridgeable_gap && off + want <= read_until;
        }

        /// Read up to `want` bytes from the open stream into `dst`; advances the frontier.
        size_t readInto(char * dst, size_t want);
        /// Discard up to `gap` bytes on the stream (over-read) to advance over a hole.
        size_t skipForward(size_t gap, size_t block_bytes);

        struct DrainResult
        {
            size_t bytes = 0;   /// bytes actually drained
            bool failed = false;   /// a read error interrupted the drain
        };
        /// If only a tail <= `max_tail` remains to the bound, read it out so the connection
        /// completes (pool-reusable). The drained bytes are discarded (keep-alive only), so a read
        /// error here must not fail the query: it is caught, logged, and reported via
        /// `DrainResult::failed`. Best-effort -- never throws.
        DrainResult drainTail(size_t max_tail, size_t block_bytes, LoggerPtr log) noexcept;
    };

    /// At known size, EOF is `position >= totalSize`. At unknown size, a short
    /// source read latches `reached_eof`; a backward `seek` clears it. A
    /// `read_until` bound caps EOF earlier.
    bool atEnd() const
    {
        if (reached_eof || (read_until && position >= *read_until))
            return true;
        return !offset_map.hasUnknownSize() && position >= totalSize();
    }

    /// Predicted forward reach as a logical end position, clamped to the file end.
    size_t clampReach(size_t reach, size_t logical_pos) const;
    /// Open a long connection now? True when a slot budget is configured, none is held,
    /// and the estimator predicts the read continues past this window.
    bool shouldOpenLongConnection() const;
    /// Acquire a slot and open a held connection on `object` at `object_offset`; false if
    /// no slot was available (caller falls back to a one-shot read).
    bool tryOpenLongConnection(const StoredObject & object, size_t object_offset);
    /// Serve one window (<= `want`) from the held connection, bridging a small leading gap;
    /// releases the connection if it reaches its bound. Precondition: `canContinue`.
    size_t serveFromLongConnection(size_t object_offset, size_t want, char * dst);
    /// One-shot bounded read (the stateless path): open, seek, read `want` into `dst`.
    size_t readOneShot(const StoredObject & object, size_t object_offset, size_t want, char * dst);
    /// Drop the held connection: drain a small tail to complete it, else account it incomplete.
    void dropLongConnection();

    std::shared_ptr<IFileBasedSourceReader> source;
    OffsetMap offset_map;
    String log_file_path;
    size_t block_size;
    size_t position = 0;
    bool reached_eof = false;
    /// Hard upper bound on the logical read position; `nullopt` = read to end.
    std::optional<size_t> read_until;

    /// Held source connection reused across sequential windows; empty when none is open.
    std::optional<LongConnection> long_conn;
    /// Forward-reach estimator, fed `recordReadRange`/`recordSeek`; drives the open-long decision.
    ReadContinuityTracker continuity_tracker;
    /// Connection-reuse budget; null disables long connections (the stateless path).
    std::shared_ptr<LongConnectionLimit> long_connection_limit;
    size_t min_bytes_for_seek;
    size_t max_tail_for_drain;

    Stats stats;
    CurrentMetrics::Increment active_metric;  /// the ReaderExecutorActive gauge, for the lifetime

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
