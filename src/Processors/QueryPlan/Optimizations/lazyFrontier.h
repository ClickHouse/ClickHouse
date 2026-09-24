#pragma once

#include <Processors/QueryPlan/Optimizations/mergedPlanDAG.h>

namespace DB::QueryPlanOptimizations
{

/// What becomes of one value of a `Limit -> ... -> sources` subtree once the heavy columns are deferred
/// past the `LIMIT`. The two sides are asked separately - the main branch computes what is needed below,
/// the branch above the `LIMIT` computes what is needed there - and a value can well be needed on both.
struct Placement
{
    /// Whether the main branch computes it, because a filter, a join condition, the sort order or a
    /// crossing column needs it there.
    bool computed_below = false;

    /// Where whatever reads it above the `LIMIT` gets it from. Nothing more is needed for the rows a join
    /// left unmatched: whether the value is gated, and by which join, is a property of the DAG rather
    /// than of this decision - see `MergedPlanDAG::getNearestStuffing` - and it applies to a column the
    /// lazy read returns just as much as to a recomputed one.
    enum class Above : uint8_t
    {
        No,         /// nothing above the `LIMIT` reads it
        Crossing,   /// the main branch hands it over as a column of the block the `LIMIT` cuts
        LazyRead,   /// a second, row-addressed read of its source returns it
        Recomputed, /// computed again there, from the values that cross and the ones read lazily
    };

    Above above = Above::No;

    /// `Crossing` is the one answer that needs the value below as well - the other three are free of
    /// each other, and `computed_below` with `Recomputed` is the case this is all for: a value the
    /// filter needs below the `LIMIT` and the result needs again above it.
    bool isConsistent() const { return above != Above::Crossing || computed_below; }
};

/// Where each value of the subtree gets computed.
///
/// The currency is columns crossing the plan below the `LIMIT`, not arithmetic: a crossing column is read
/// for every scanned row, goes through the sort, and a join replicates it, while recomputing a value
/// above the `LIMIT` touches at most `limit` rows. So a deferred value is recomputed from whatever is
/// already at hand - a column the main branch computes anyway, or a source column the lazy read
/// produces - and only a value that cannot be recomputed crosses.
struct LazyFrontier
{
    /// Values with no entry are not involved: nothing above or below the `LIMIT` reads them.
    std::unordered_map<const ActionsDAG::Node *, Placement> placement;

    Placement at(const ActionsDAG::Node * node) const;

    /// Whether anything is computed above the `LIMIT` at all. When false the caller has nothing to gain.
    bool defersAnything() const;
};

/// `eager_output_positions` are the outputs of `merged` a caller needs below the `LIMIT` anyway, i.e. the
/// sort description. Filters and join conditions are taken from `merged` itself.
/// `lazy_sources` says which sources support a second, row-addressed read; a value reading one that does
/// not is computed below the `LIMIT` and crosses instead.
///
/// `masked_stuffings` are the stuffings the caller has a mask for above the `LIMIT`. A value gated by one
/// of them is recomputed there as `if(mask, value, default)`, which evaluates it on the rows the join
/// stuffed as well - so only a value that cannot throw is recomputed this way, which is what a function
/// not suitable for short-circuit evaluation is: cheap and never throwing. Any other gated value, and one
/// whose stuffing has no mask, is computed below the `LIMIT`, where the join stuffs it itself.
LazyFrontier chooseLazyFrontier(
    const MergedPlanDAG & merged,
    const std::vector<size_t> & eager_output_positions,
    const std::vector<bool> & lazy_sources,
    const std::unordered_set<const MergedPlanDAG::Stuffing *> & masked_stuffings = {});

/// The inputs each source's lazy read has to produce, indexed by source. Derived from the frontier, so
/// that the one answer lives in one place.
std::vector<NodeSet> collectLazyReads(const MergedPlanDAG & merged, const LazyFrontier & frontier);

}
