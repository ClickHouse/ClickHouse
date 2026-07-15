#pragma once

#include <IO/CoverageMap.h>
#include <IO/ChainedBuffers.h>
#include <IO/ICacheProvider.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB
{

/// The explicit work of ONE look-ahead plan, computed once from the immutable
/// `CoverageMap`. Collapses fetch/fill/promote into a single `Retrieve`
/// job kind and predicts what each `readNextWindow` returns. Pure description -
/// no buffers, no I/O; the model lives in `ReaderExecutor`'s class comment.
struct PlanSchedule
{
    enum class Purpose : uint8_t
    {
        User,      /// inside the plan span - the only bytes readNextWindow returns
        FillOnly,  /// cell slack outside the span - fetched/filled, never served
    };

    enum class Source : uint8_t
    {
        Remote,       /// a source connection (may bridge small resident holes)
        HandedChain,  /// promote: bytes already in hand (the served chain), written up
    };

    /// The typed decomposition of the fill region (purpose x residency), one
    /// entry per maximal segment where both are constant. `tier_entry` is
    /// valid only when `resident`.
    struct TypedRange
    {
        ByteRange range;            /// physical, plan coords
        Purpose purpose = Purpose::User;
        bool resident = false;      /// true: served from a tier; false: gap (remote)
        size_t tier_entry = CoverageMap::npos;
    };

    /// One aligned-miss cell to populate, identified by its tier-entry index
    /// (into `CoverageMap::entries` / `ReadPlan::tiers`) and the cell range.
    struct WriteTarget
    {
        size_t entry = 0;
        ByteRange cell;
    };

    /// One unit of background work: move `range` from `source` into the `into`
    /// cells, optionally retaining it for the serve. Reaches a READY milestone
    /// (bytes fetched, serve may proceed) then a DONE milestone (filled, handles
    /// released).
    struct Retrieve
    {
        ByteRange range;                              /// physical, plan coords
        Source source = Source::Remote;
        VectorWithMemoryTracking<WriteTarget> into;   /// cells to populate
        /// May the background run this job ahead of the serve? `Remote` fills depend on nothing
        /// but the source; `HandedChain` takes the SERVE's output as its input, so it is
        /// inherently a serve-front (sync) job. The fg/bg partition of the work, as
        /// schedule data.
        bool ahead_eligible = false;
        /// The sub-ranges of `range` to read from the SOURCE (`Remote` only, empty otherwise).
        /// `range` merges adjacent cell-aligned gaps, so it can span an embedded resident
        /// region - served from its tier, never SCHEDULED as a source read; the runs split at
        /// every one. (Whether the executor reads THROUGH one at run time is a display-state
        /// decision, not a schedule property.) Executable as written: the executor fetches
        /// these runs verbatim, with no geometry query at serve time.
        VectorWithMemoryTracking<ByteRange> fetch_runs;
    };

    /// One readNextWindow output and the retrieval it waits on (its READY
    /// milestone). `require_retrieve` is empty for a cache hit.
    /// The serve-side MAP (not an instruction stream): one classification run - a window
    /// must not cross a hit/gap boundary, or the pump would be keyed to an ambiguous job -
    /// wired to the job that fills it (`require_retrieve` empty = a resident hit run).
    struct ServeRun
    {
        ByteRange output;                             /// physical / plan coords (same space as Retrieve.range and position_phys)
        std::optional<size_t> require_retrieve;       /// index into `retrieves`
        /// The serve GRANULARITY (the consumer's ask, a maximum - the serve returns any
        /// non-empty ready prefix): a hit run is block-bounded (no remote open to amortise,
        /// the bound is in-flight memory per call); a job run is window-bounded (the fetch
        /// it may pump amortises over it). Pressure-scaled at plan build.
        size_t serve_bound = 0;
    };

    VectorWithMemoryTracking<TypedRange> ranges;
    VectorWithMemoryTracking<Retrieve> retrieves;
    VectorWithMemoryTracking<ServeRun> serve_runs;
};

/// Describe the work of the plan `geometry` over its own span
/// `[plan_start, plan_end)` (physical coords). Pure function of the geometry;
/// the serve sizes (pressure-scaled by the caller) become each run's `serve_bound`.
///
/// CACHE-CHAIN POLICY - correctness for many tiers, performance for one. With
/// several populating caches the schedule stays CORRECT but optimises for the
/// BOTTOM populating tier alone; upper tiers are a serve bonus (a hit serves
/// from its fastest holder), not a coordination target. There is deliberately
/// NO cross-tier down-fill job: a faster tier's resident range over a lower
/// cell leaves the lower segment partial (the demand read-through completes an
/// interior hole from the source; a tail hole heals once the upper tier evicts
/// and the range becomes a plain miss). One populating tier - the production
/// shape - has no such ranges and takes the unimpaired path.
PlanSchedule buildSchedule(
    const CoverageMap & geometry,
    size_t serve_window_bytes,
    size_t serve_block_bytes);

}
