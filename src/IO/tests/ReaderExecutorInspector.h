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
    size_t inflightPrefetchSize() const { return ex.machine ? ex.machine->physical_window.size : 0; }
    size_t abandonedPrefetchCount() const { return ex.abandoned_machines.size(); }
    bool machineHasLongConn() const { return ex.machine && ex.machine->long_conn.has_value(); }

    /// The current look-ahead plan geometry (null until the first plan is built),
    /// for validating `buildSchedule` against the live walk.
    std::shared_ptr<const CoverageMap> planGeometry() const { return ex.read_plan.geometry(); }

    /// The plan's serve horizon.
    size_t planEnd() const { return ex.read_plan.geometry() ? ex.read_plan.geometry()->plan_end : 0; }

    /// Number of plan (re)builds so far -- one per `observeAndSchedule`. Lets a
    /// test assert the plan is REUSED across read-extent advances (count flat).
    UInt64 observationCount() const { return ex.stats.get(ReaderExecutor::Stats::Observations); }

    /// The per-job status sidecar, allocated 1:1 with the schedule's jobs.
    bool retrieveStatusMatchesSchedule() const
    {
        return ex.read_plan.retrieve_status.size() == ex.read_plan.schedule.retrieves.size();
    }
    size_t retrieveStatusSize() const { return ex.read_plan.retrieve_status.size(); }

    /// The serve map (runs / per-job progress).
    size_t serveRunCount() const { return ex.read_plan.schedule.serve_runs.size(); }
    ByteRange serveRunOutput(size_t i) const { return ex.read_plan.schedule.serve_runs[i].output; }
    size_t serveRunAt(size_t phys) const { return ex.serveRunAt(phys); }
    /// Job-RELATIVE launch progress (bytes from the job's range start).
    size_t retrieveLaunchProgress(size_t i) const
    {
        return ex.launchProgress(i) - ex.read_plan.schedule.retrieves[i].range.offset;
    }

    /// The continuity estimator's predicted reach after the last plan feed.
    size_t predictedReach() const { return ex.continuity_tracker.predictedReach(); }

    /// Bank injection / inspection (the display's overflow cell). `bankBytes` constructs the
    /// state the wait-bank and overflow-bank paths produce - including a HOLEY bank, whose
    /// production trigger (a sibling-led wait returning short between two served cells) needs
    /// a cross-executor race - so a test can pin the display's frontier/read agreement on it.
    void bankBytes(size_t ri, size_t logical_offset, std::string_view bytes)
    {
        auto buf = std::make_shared<OwnedChainedBuffer>(bytes.size());
        std::memcpy(buf->data(), bytes.data(), bytes.size());
        ChainedBuffers chunk;
        chunk.append(ChainedBufferNode{buf, 0, bytes.size(), logical_offset});
        ex.read_plan.retrieve_status[ri].ready_bytes.append(std::move(chunk));
    }
    const VectorWithMemoryTracking<ByteRange> & bankIntervals(size_t ri) const
    {
        return ex.read_plan.retrieve_status[ri].ready_bytes.getIntervals();
    }
    /// The schedule job whose range holds `phys`, or `size_t(-1)`.
    size_t retrieveIndexAt(size_t phys) const
    {
        for (size_t i = 0; i < ex.read_plan.schedule.retrieves.size(); ++i)
        {
            const auto & r = ex.read_plan.schedule.retrieves[i].range;
            if (phys >= r.offset && phys < r.end())
                return i;
        }
        return size_t(-1);
    }
    /// Latch size-unknown EOF as a collected machine's short read would (the merge at
    /// collect), without having to stage the pool machine + refused-put race.
    void latchEof() { ex.reached_eof = true; }
    /// Drive one serve of the cursor step at `logical_pos`, bypassing `readNextWindow`'s
    /// pre-read EOF gate - the engine runs below a latched EOF only via the machine-drain
    /// branch, which needs an in-flight machine a unit test cannot hold still.
    ChainedBuffers serveWindowAt(size_t logical_pos)
    {
        const size_t phys = logical_pos + ex.data_start_offset;
        ex.prepareCursor(phys);
        return ex.serveWindow(phys);
    }

    /// Long-connection probes.
    bool hasLongConn() const { return ex.fill_lane.conn.has_value(); }
    size_t longConnPosition() const { return ex.fill_lane.conn ? ex.fill_lane.conn->current_position : 0; }
    size_t longConnBound() const { return ex.fill_lane.conn ? ex.fill_lane.conn->read_until : 0; }
    bool longConnServes(const String & path) const { return ex.fill_lane.conn && ex.fill_lane.conn->servesObject(path); }
    bool longConnCanContinue(size_t off, size_t want) const
    {
        return ex.fill_lane.conn && ex.fill_lane.conn->canContinue(off, want, ex.min_bytes_for_seek);
    }

    /// Wrappers around the private reach / open-decision math.
    bool shouldOpenLong(size_t phys_off) const { return ex.shouldOpenLong(phys_off); }
    size_t clampReach(size_t reach, size_t phys_off) const { return ex.clampReach(reach, phys_off); }
    size_t scheduleLookaheadReach(size_t phys_off) const { return ex.scheduleLookaheadReach(phys_off); }

    /// Counters.
    UInt64 incompleteConnections() const { return ex.stats.get(ReaderExecutor::Stats::IncompleteConnections); }
    UInt64 sourceRequests() const { return ex.stats.get(ReaderExecutor::Stats::SourceRequests); }
    UInt64 prefetchHits() const { return ex.stats.get(ReaderExecutor::Stats::PrefetchHits); }
    UInt64 syncReadMicros() const { return ex.stats.get(ReaderExecutor::Stats::SyncReadMicroseconds); }

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
        ex.openLong(ex.fill_lane.conn, pr.object, pr.object_offset, read_end, std::move(slot), ex.stats);
    }

    ChainedBuffers serveFromLong(size_t phys_offset, size_t want)
    {
        auto ranges = ex.offset_map.map(ByteRange{phys_offset, want});
        chassert(!ranges.empty());
        const auto & pr = ranges.front();
        auto blocks = ReaderExecutor::allocateBlocks(want, ex.block_size, {});
        return ex.serveFromLong(ex.fill_lane.conn, pr.object_offset, std::move(blocks), phys_offset, /*stop=*/nullptr, ex.stats);
    }

    void dropLong() { ex.dropLong(ex.fill_lane.conn, ex.stats); }

private:
    ReaderExecutor & ex;
};

inline ReaderExecutorInspector inspect(ReaderExecutor & e)
{
    return ReaderExecutorInspector{e};
}

}
