#pragma once

#include <IO/ReaderExecutor.h>

namespace DB
{

/// Test-only observability for `ReaderExecutor`. A friend that wraps a single
/// executor and exposes its private state - plus a few internal drivers - to unit
/// tests, so the production class carries no test-only surface of its own.
/// Header-only: included by the gtest TUs, never by production code. Obtain one
/// via `inspect(executor)`; e.g. `inspect(ex).hasLongConn()`.
class ReaderExecutorInspector
{
public:
    explicit ReaderExecutorInspector(ReaderExecutor & e) : ex(e) {}

    /// Machine / prefetch state.
    bool hasInflightPrefetch() const { return ex.machine != nullptr; }
    size_t inflightPrefetchSize() const { return ex.machine ? ex.machine->requested_range.size : 0; }
    size_t abandonedPrefetchCount() const { return ex.abandoned_machines.size(); }
    bool machineHasLongConn() const { return ex.machine && ex.machine->long_conn.has_value(); }

    /// The current look-ahead plan geometry (null until the first plan is built),
    /// for validating `buildSchedule` against the live walk.
    std::shared_ptr<const CoverageMap> planGeometry() const { return ex.read_plan.geometry(); }

    /// The per-job status sidecar, allocated 1:1 with the schedule's jobs.
    bool retrieveStatusMatchesSchedule() const
    {
        return ex.read_plan.retrieve_status.size() == ex.read_plan.schedule.retrieves.size();
    }
    size_t retrieveStatusSize() const { return ex.read_plan.retrieve_status.size(); }

    /// Assert-spine shadow state (cursor / steps / per-job phase). The cursor and
    /// phases are only maintained under `DEBUG_OR_SANITIZER_BUILD`.
    size_t cursor() const { return ex.read_plan.cursor; }
    size_t stepCount() const { return ex.read_plan.schedule.steps.size(); }
    ByteRange stepOutput(size_t i) const { return ex.read_plan.schedule.steps[i].output; }
    int retrievePhase(size_t i) const { return static_cast<int>(ex.read_plan.retrieve_status[i].phase); }

    /// The continuity estimator's predicted reach after the last plan feed.
    size_t predictedReach() const { return ex.continuity_tracker.predictedReach(); }

    /// Long-connection probes.
    bool hasLongConn() const { return ex.long_conn.has_value(); }
    size_t longConnPosition() const { return ex.long_conn ? ex.long_conn->current_position : 0; }
    size_t longConnBound() const { return ex.long_conn ? ex.long_conn->read_until : 0; }
    bool longConnServes(const String & path) const { return ex.long_conn && ex.long_conn->servesObject(path); }
    bool longConnCanContinue(size_t off, size_t want) const
    {
        return ex.long_conn && ex.long_conn->canContinue(off, want, ex.min_bytes_for_seek);
    }

    /// Wrappers around the private reach / open-decision math.
    bool shouldOpenLong(size_t phys_off) const { return ex.shouldOpenLong(phys_off); }
    size_t clampReach(size_t reach, size_t phys_off) const { return ex.clampReach(reach, phys_off); }
    size_t scheduleLookaheadReach(size_t phys_off) const { return ex.scheduleLookaheadReach(phys_off); }

    /// Counters.
    UInt64 incompleteConnections() const { return ex.stats.get(ReaderExecutor::Stats::IncompleteConnections); }
    UInt64 sourceRequests() const { return ex.stats.get(ReaderExecutor::Stats::SourceRequests); }

    /// Drivers that exercise the long-connection mechanics directly. The
    /// `ex.`-qualified calls target `ReaderExecutor`'s private methods, not the
    /// same-named inspector methods.
    void openLong(size_t phys_offset, size_t reach)
    {
        auto ranges = ex.offset_map.map(ByteRange{phys_offset, 1});
        chassert(!ranges.empty());
        const auto & pr = ranges.front();
        const size_t obj_file_offset = phys_offset - pr.object_offset;
        const size_t phys_bound = std::min<size_t>(ex.clampReach(reach, phys_offset), obj_file_offset + pr.object.bytes_size);
        const size_t read_end = phys_bound - obj_file_offset;
        LongConnectionSlot slot = ex.long_connection_limit
            ? ex.long_connection_limit->tryAcquire(ex.long_connection_limit)
            : LongConnectionSlot{};
        ex.openLong(ex.long_conn, pr.object, pr.object_offset, read_end, std::move(slot), ex.stats);
    }

    ChainedBuffers serveFromLong(size_t phys_offset, size_t want)
    {
        auto ranges = ex.offset_map.map(ByteRange{phys_offset, want});
        chassert(!ranges.empty());
        const auto & pr = ranges.front();
        auto blocks = ReaderExecutor::allocateBlocks(want, ex.block_size, {});
        return ex.serveFromLong(ex.long_conn, pr.object_offset, std::move(blocks), phys_offset, /*stop=*/nullptr, ex.stats);
    }

    void dropLong() { ex.dropLong(ex.long_conn, ex.stats); }

private:
    ReaderExecutor & ex;
};

inline ReaderExecutorInspector inspect(ReaderExecutor & e)
{
    return ReaderExecutorInspector{e};
}

}
