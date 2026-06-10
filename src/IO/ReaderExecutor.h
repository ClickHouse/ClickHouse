#pragma once

#include <IO/OffsetMap.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/BufferWithOwnMemory.h>

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
/// bytes from an `IFileBasedSourceReader`, one block at a time, into an owned buffer.
/// Drives the experimental `use_reader_executor` read path. One instance per
/// column-stream; not thread-safe.
class ReaderExecutor
{
public:
    static constexpr size_t DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024; /// 1 MiB

    ReaderExecutor(
        std::shared_ptr<IFileBasedSourceReader> source,
        const StoredObjects & objects,
        size_t block_size = DEFAULT_BLOCK_SIZE);

    ~ReaderExecutor();

    /// A contiguous run of bytes starting at the current position. `data` points
    /// into the executor's own block buffer and stays valid only until the next
    /// `readNextChunk` / `seek`. `size == 0` means EOF.
    struct Chunk
    {
        const char * data = nullptr;
        size_t size = 0;
        size_t logical_offset = 0;
    };

    /// Read the next block (<= `block_size`, clamped to the current object's end
    /// for known-size objects), advancing the position by the bytes read.
    Chunk readNextChunk();

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
    /// drift and every update is instantly observable. The cache / connection counters
    /// have no caller in this minimal slice, so they stay 0 until their features land.
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
            NumCounters,
        };

        void add(Counter c, UInt64 value = 1);
        UInt64 get(Counter c) const { return values[c]; }

        /// Roll a future transient sub-executor's tally into the parent without re-emitting
        /// (each counter was already emitted at its `add`).
        Stats & operator+=(const Stats & o)
        {
            for (size_t i = 0; i < NumCounters; ++i)
                values[i] += o.values[i];
            return *this;
        }

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

    /// At known size, EOF is `position >= totalSize`. At unknown size, a short
    /// source read latches `reached_eof`; a backward `seek` clears it. A
    /// `read_until` bound caps EOF earlier.
    bool atEnd() const
    {
        if (reached_eof || (read_until && position >= *read_until))
            return true;
        return !offset_map.hasUnknownSize() && position >= totalSize();
    }

    std::shared_ptr<IFileBasedSourceReader> source;
    OffsetMap offset_map;
    String log_file_path;
    size_t block_size;
    size_t position = 0;
    bool reached_eof = false;
    /// Hard upper bound on the logical read position; `nullopt` = read to end.
    std::optional<size_t> read_until;

    /// Backs the bytes returned by the latest `readNextChunk`.
    Memory<> block;

    Stats stats;
    CurrentMetrics::Increment active_metric;  /// CurrentMetrics::ReaderExecutorActive for the lifetime

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
