#pragma once

#include <Core/Types.h>
#include <Common/VectorWithMemoryTracking.h>

#include <memory>
#include <mutex>
#include <set>

namespace DB
{

class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;
struct EventFD;

/// The cursor that a bounded streaming read (`STREAM BOUNDED`) advanced to, see `IStorage::reportsStreamingCursor`.
/// It exists only for the incremental refresh of a refreshable materialized view (`APPEND INCREMENTAL`), which
/// persists it to resume the next refresh. Other streaming queries must not rely on it.
///
/// A read of a composite table (`Merge`, `Distributed`) gives each of its parts a child holder, which places the cursor
/// of the part under its own key path (e.g. database and table) in the parent.
struct StreamingCursor
{
    std::mutex mutex;
    CursorTreeNodePtr tree;

    /// Merge the cursor of a finished read into this holder (or into the parent under `path`). The trees are merged
    /// in the form they are persisted in, where a key with a dot is split into a path.
    ///
    /// The holder starts from the cursor of the previous refresh, so that a part of a composite source that is not read
    /// this time keeps its position. The first cursor reported under a path replaces the subtree of the previous refresh
    /// at that path (the reads of the parallel streams of one table are merged together), so the entries that the read
    /// no longer has (e.g. dropped partitions) are retired instead of accumulating forever.
    void merge(const CursorTreeNodePtr & cursor);

    static std::shared_ptr<StreamingCursor> createChild(std::shared_ptr<StreamingCursor> parent, Strings path);

    /// A read that is going to report its cursor to the holder, from its creation until it reports it or finishes.
    ///
    /// A query can finish its output before all its reads report: e.g. a `DISTINCT` by constants closes the parallel
    /// streams of a table when one of them gives the row, and each of them reports from its own shutdown later. A target
    /// that commits the cursor along with its data (e.g. `Iceberg`) does it when its input is finished, so its input
    /// waits for the pending reads first, see `WaitForStreamingCursorTransform`.
    class PendingReport
    {
    public:
        PendingReport() = default;
        explicit PendingReport(std::shared_ptr<StreamingCursor> holder_);
        PendingReport(PendingReport && other) noexcept;
        PendingReport & operator=(PendingReport && other) noexcept;
        ~PendingReport();

        /// The read reported its cursor or finished without it (e.g. it failed or was cancelled).
        void finish() noexcept;

    private:
        /// The root holder, which counts the pending reads.
        std::shared_ptr<StreamingCursor> holder;
    };

    /// Whether some reads did not report yet (the count is in the root holder).
    bool hasPendingReports();
    /// Write to the event once there are no pending reads (right away if there are none). Every waiter has its own event,
    /// as the executor of a query cannot poll one descriptor for several processors.
    void notifyWhenNoPendingReports(std::shared_ptr<EventFD> event);

    StreamingCursor();
    ~StreamingCursor();

private:
    void mergeAt(const Strings & sub_path, const CursorTreeNodePtr & cursor);
    StreamingCursor & root();

    std::shared_ptr<StreamingCursor> parent;
    Strings path;
    /// The paths (split at dots) under which a cursor was already reported, only in the root holder.
    std::set<Strings> reported_paths;

    /// The pending reads, only in the root holder, and the events to write to once their count drops to zero.
    size_t pending_reports = 0;
    VectorWithMemoryTracking<std::shared_ptr<EventFD>> no_pending_reports_waiters;
};

}
