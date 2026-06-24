#pragma once

#include <IO/ChainedBuffers.h>
#include <IO/ICacheProvider.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// One cache tier's RESIDENT geometry over the look-ahead window, for one
/// object-piece: the file-level ranges this tier holds resident plus the
/// cache-ALIGNED miss ranges (the gap-fetch + write targets). Holds NO cache
/// buffer - those live in the same plan's `ReaderExecutor::ReadPlan::bufs`.
/// `aligned_miss` is clamped only to the object end (it may extend past the
/// plan span so a touched cell is fully populated).
struct GeometryEntry
{
    CacheTier tier{};
    /// Grid the fetch rounds to at each edge of a miss run
    /// (`ICacheProvider::fetch{Head,Tail}Alignment`); `1` = no over-read.
    size_t head_align = 1;
    size_t tail_align = 1;
    /// Cell is written WHOLE (first-writer-wins, e.g. page) vs incrementally
    /// appended (fs). A whole-cell tier fills only when a connection covers the
    /// entire cell. Separate from `tail_align` so a tier can fetch a wide cell
    /// without being first-writer-wins.
    bool whole_cell = false;
    VectorWithMemoryTracking<ByteRange> resident;
    VectorWithMemoryTracking<ByteRange> aligned_miss;
};

/// The IMMUTABLE geometry of one look-ahead plan: the resident layout + span,
/// queried positionally (RESIDENT run vs GAP). Built once by
/// `ReaderExecutor::observeAndSchedule`, never mutated after publish, exposed
/// as a `shared_ptr<const>` snapshot. `entries` is in cache-tier priority
/// order, 1:1 POSITIONAL with `ReadPlan::bufs`. Empty / `plan_end == plan_start`
/// means no valid plan.
struct CoverageMap
{
    static constexpr size_t npos = static_cast<size_t>(-1);

    /// What the plan holds at an offset: the index of the entry whose resident
    /// range covers it (`npos` = gap), its tier, and the end of that contiguous
    /// resident run.
    struct Resident
    {
        size_t entry = npos;
        CacheTier tier{};
        size_t run_end = 0;
        bool resident() const { return entry != npos; }
    };

    bool covers(ByteRange w) const
    {
        return plan_end > plan_start && w.offset >= plan_start && w.end() <= plan_end;
    }

    Resident residentAt(size_t offset) const;

    /// First non-resident offset at or after `from`; `plan_end` when everything
    /// to the plan end is resident.
    size_t nextGapStart(size_t from) const;

    /// End of the gap starting at `gap_start`: the next resident range's start,
    /// or `plan_end`.
    size_t gapEnd(size_t gap_start) const;

    /// The window to FETCH to serve `req`: `req` rounded OUT to the cache cell
    /// at each edge (it may start LEFT of `req.offset` and end past `req.end()`;
    /// the caller slices back). The widening is BOUNDED by each tier's alignment
    /// grid, NOT the coalesced miss run, and is clamped into the run so it never
    /// reaches resident bytes.
    ByteRange fetchWindowAt(ByteRange req) const;

    /// How far a live connection opened at `from` would stream before it must
    /// reopen: gaps plus resident runs no larger than `min_gap` (bridged),
    /// stopping at the first larger one or `plan_end`. A reach no larger than a
    /// window means a one-shot serves it and no lease is warranted.
    size_t streamReach(size_t from, size_t min_gap) const;

    size_t plan_start = 0;  /// physical (header-inclusive) coords
    size_t plan_end = 0;    /// [plan_start, plan_end) -- the serve/schedule horizon
    /// The pin horizon: the rightmost byte of any pinned cache state (folded hit
    /// segments plus aligned-miss cells), always `>= plan_end`. Everything -- the
    /// serve/schedule queries AND the plan-reuse gate -- keys off `plan_end` (the
    /// serve horizon): keying the reuse gate off `pinned_end` instead would leave a
    /// dead zone `[plan_end, pinned_end)` where the serve stops but the re-plan never
    /// fires. `pinned_end` only documents how far the pins reach (for eviction
    /// reasoning / test observability). Equal to `plan_end` unless the generalized
    /// plan-window extension is enabled.
    size_t pinned_end = 0;
    VectorWithMemoryTracking<GeometryEntry> entries;

    /// `MemoryPressureMonitor` level sampled ONCE at plan build; reads within
    /// the plan use it instead of re-querying per call.
    MemoryPressureLevel pressure_level{};
};

}
