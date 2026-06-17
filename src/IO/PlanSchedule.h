#pragma once

#include <IO/CoverageMap.h>
#include <IO/ChainedBuffers.h>
#include <IO/ICacheProvider.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB
{

/// The explicit work of ONE look-ahead plan, computed once from the immutable
/// `CoverageMap`. Collapses fetch/fill/promote into a single `Retrieve`
/// job kind and predicts what each `readNextWindow` returns. Pure description -
/// no buffers, no I/O; see `tmp/reader-executor-unified-plan/DESIGN.md`.
struct PlanSchedule
{
    enum class Purpose : uint8_t
    {
        User,      /// inside the request/extent - the only bytes readNextWindow returns
        FillOnly,  /// alignment slack a lower tier needs but the request does not cover
    };

    enum class Source : uint8_t
    {
        Remote,          /// a source connection (may bridge small resident holes)
        UpperCacheRead,  /// read a FillOnly range from a faster resident tier, no remote
        HandedChain,      /// promote: bytes already in hand (the served chain), written up
    };

    /// The typed decomposition of the fill region (purpose x residency), one
    /// entry per maximal segment where both are constant. `tier_entry`/`tier`
    /// are valid only when `resident`.
    struct TypedRange
    {
        ByteRange range;            /// physical, plan coords
        Purpose purpose = Purpose::User;
        bool resident = false;      /// true: served from a tier; false: gap (remote)
        size_t tier_entry = CoverageMap::npos;
        CacheTier tier{};
    };

    /// One aligned-miss cell to populate, identified by its tier-entry index
    /// (into `CoverageMap::entries` / `ReadPlan::bufs`) and the cell range.
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
        /// The tier the bytes are read from, for the non-`Remote` sources:
        /// `UpperCacheRead` reads its `range` from this tier; `HandedChain`
        /// records the tier the served chain came from.
        CacheTier upper_source_tier{};
        VectorWithMemoryTracking<WriteTarget> into;   /// cells to populate
        bool retain_for_serve = false;                /// User range vs FillOnly
        VectorWithMemoryTracking<size_t> deps;        /// same-segment predecessors (natural order)
    };

    /// One readNextWindow output and the retrieval it waits on (its READY
    /// milestone). `require_retrieve` is empty for a cache hit.
    struct Step
    {
        ByteRange output;                             /// physical / plan coords (same space as Retrieve.range and position_phys)
        std::optional<size_t> require_retrieve;       /// index into `retrieves`
    };

    VectorWithMemoryTracking<TypedRange> ranges;
    VectorWithMemoryTracking<Retrieve> retrieves;
    VectorWithMemoryTracking<Step> steps;
};

/// Describe the work of the plan `geometry` for the half-open logical request
/// `request_extent` (physical coords here; the caller adds the encryption-header
/// shift). Pure function of the geometry; `pressure` and `min_bytes_for_seek`
/// shape connection width and streaming footprint.
PlanSchedule buildSchedule(
    const CoverageMap & geometry,
    ByteRange request_extent,
    MemoryPressureLevel pressure,
    size_t min_bytes_for_seek);

}
