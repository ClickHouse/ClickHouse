#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/MarkRange.h>

#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>

#include <memory>
#include <optional>


namespace DB
{

class IDataPartStorage;
struct StoredObject;

/// Result of opening a remote storage connection for a byte range.
struct RemoteConnectionResult
{
    /// The raw socket file descriptor for the established HTTP connection.
    /// Future: RRM can register it in epoll for async reading.
    int fd = -1;

    /// The HTTP response body stream (from Poco/AWS SDK).
    /// Always use this for reading — it correctly handles internal
    /// buffering and chunked transfer encoding.
    /// The raw FD is only useful for future epoll registration.
    std::istream * body_stream = nullptr;

    /// Holds the HTTP session and response stream alive.
    /// The connection is closed when this is destroyed.
    std::shared_ptr<void> session_holder;
};

/// Factory that opens a remote storage connection for a given byte range.
/// Captures storage-specific context (S3 client, bucket, key, etc.).
/// RRM calls this to open connections at its own pace.
using RemoteConnectionFactory = std::function<RemoteConnectionResult(
    size_t range_begin, size_t range_end, size_t attempt)>;

/// Describes the logical context of a file read for resource scheduling.
/// RRM uses this to group and order reads: prewhere columns are read before
/// rest columns for the same part and mark ranges.
///
/// Always heap-allocated and shared via `ReadScopePtr`.  Use `ReadScope::create`
/// to construct; the constructor is private to guarantee `shared_from_this` works.
struct ReadScope : public std::enable_shared_from_this<ReadScope>
{
    /// Unique identifier for the part being read (e.g. "database/table/all_1_5_1").
    /// Used as a grouping key — reads with the same `part_id` belong to the same part.
    String part_id;

    /// Mark ranges being read by this stream.
    MarkRanges mark_ranges;

    /// Reading phase determines scheduling priority and ordering.
    enum Phase : uint8_t
    {
        Prewhere,  /// Prewhere columns — read first, byte ranges known upfront.
        Rest,      /// Remaining columns — read after prewhere filtering.
    };
    Phase phase = Rest;

    /// Compressed file byte range for each granule across all mark_ranges.
    /// For mark_ranges = {[2,5), [10,12)}, granule_ranges has 5 entries:
    /// granules 2, 3, 4, 10, 11 — each with its own {begin, end} in the .bin file.
    struct ByteRange
    {
        size_t begin;
        size_t end;
    };
    std::vector<ByteRange> granule_ranges;

    /// Thread group of the query that created this scope.
    /// Captured at scope creation time so that async prefetch threads
    /// can attach to the originating query's context.
    ThreadGroupPtr thread_group;

    String toString() const;

    static std::shared_ptr<const ReadScope> create(
        String part_id_, MarkRanges mark_ranges_, Phase phase_, std::vector<ByteRange> granule_ranges_ = {});

    /// Return a scope with `granule_ranges` clipped and translated to
    /// object-local coordinates.  Returns `shared_from_this` when no
    /// adjustment is needed.  Ranges entirely outside the object are
    /// dropped; ranges that straddle an object boundary are clamped.
    std::shared_ptr<const ReadScope> adjustForObject(size_t object_start_offset, size_t object_size) const;

private:
    /// Captures `CurrentThread::getGroup` — used by `create`.
    ReadScope(String part_id_, MarkRanges mark_ranges_, Phase phase_,
              std::vector<ByteRange> granule_ranges_);

    /// Explicit thread group — used by `adjustForObject` to preserve the original query's group.
    ReadScope(String part_id_, MarkRanges mark_ranges_, Phase phase_,
              std::vector<ByteRange> granule_ranges_, ThreadGroupPtr thread_group_);
};

using ReadScopePtr = std::shared_ptr<const ReadScope>;

/// Prototype Remote Reading Manager.
///
/// Currently a transparent proxy: logs the `ReadScope` for each file read,
/// then delegates to `IDataPartStorage::readFile` as before.
///
/// Future versions will use the scope information for resource-aware scheduling:
/// prioritizing prewhere reads, enforcing per-query budgets, managing connections.
class RemoteReadingManager
{
public:
    static RemoteReadingManager & instance();

    /// Create a ReadBuffer for reading a column data file.
    /// Sets `read_scope` on the settings so that object storage implementations
    /// call back into `createObjectReadBuffer` for the bottom-level buffer.
    std::unique_ptr<ReadBufferFromFileBase> createReadBuffer(
        ReadScopePtr scope,
        const IDataPartStorage & storage,
        const String & file_name,
        const ReadSettings & settings,
        std::optional<size_t> estimated_size);

    /// Called by object storage implementations (S3, Azure, etc.) when
    /// `ReadSettings.read_scope` is set. Receives a connection factory
    /// that RRM can call to open HTTP connections for specific byte ranges.
    /// Currently a proxy: logs the interception and returns nullptr to let
    /// the caller fall back to its default buffer creation.
    std::unique_ptr<ReadBufferFromFileBase> createObjectReadBuffer(
        const ReadScope & scope,
        const StoredObject & object,
        RemoteConnectionFactory connection_factory);

private:
    RemoteReadingManager() = default;

    LoggerPtr log = getLogger("RemoteReadingManager");
};

}
