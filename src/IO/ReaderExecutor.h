#pragma once

#include <IO/OffsetMap.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/BufferWithOwnMemory.h>

#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <base/types.h>

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
    /// Per-instance tally of the inputs to the modeled-cost KPI. Mirrors the
    /// ProfileEvents that `onSourceRead` / `onWork` increment instantly; kept per
    /// instance only to write the final summary log report in the destructor. The
    /// cache/connection fields stay 0 in this minimal slice (those features are not
    /// implemented yet); they are kept so the KPI formula is complete and lights up
    /// when the corresponding feature PRs add their increment sites.
    struct Stats
    {
        size_t source_requests = 0;          /// chunks opened and read from the source
        size_t bytes_from_source = 0;        /// physical bytes read from the source
        size_t bytes_requested = 0;          /// useful bytes delivered to the caller (KPI denominator)
        size_t incomplete_connections = 0;   /// 0: no source-connection reuse yet
        size_t cache_get_requests = 0;       /// 0: no cache tiers yet
        size_t cache_populate_requests = 0;  /// 0: no cache tiers yet
        size_t work_microseconds = 0;        /// total time spent in readNextChunk

        /// Record a source read of `bytes`: update the tally and increment the matching
        /// ProfileEvents instantly (the modeled cost by its running delta, kept exact).
        void onSourceRead(size_t bytes);
        /// Record `microseconds` spent in readNextChunk (tally + ProfileEvents).
        void onWork(size_t microseconds);

        /// Modeled I/O cost in microseconds: weighted sum with the heuristic S3 weights
        /// documented at `ProfileEvents::ReaderExecutorModeledCostMicroseconds` (30ms/source
        /// request, 5ms/incomplete connection, 20ms per MiB from source, 0.1ms/cache put,
        /// 0.05ms/cache get). A synthetic optimality proxy, not latency.
        size_t modeledCostMicroseconds() const
        {
            return 30000 * source_requests
                + 5000 * incomplete_connections
                + 20000 * bytes_from_source / (1024 * 1024)
                + 100 * cache_populate_requests
                + 50 * cache_get_requests;
        }
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
    /// Bumps `CurrentMetrics::ReaderExecutorActive` for the executor's lifetime
    /// (constructed in the .cpp where the metric symbol is declared).
    CurrentMetrics::Increment active_metric;

    LoggerPtr log = getLogger("ReaderExecutor");
};

}
