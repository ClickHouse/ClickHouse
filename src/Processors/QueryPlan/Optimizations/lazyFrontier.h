#pragma once

#include <Processors/QueryPlan/Optimizations/mergedPlanDAG.h>

namespace DB::QueryPlanOptimizations
{

/// What becomes of one value of a `Limit -> ... -> sources` subtree once the heavy columns are deferred past the
/// `LIMIT`: where it is computed - below the `LIMIT`, above it, or both. Whether the value is gated by a join that can
/// leave its side unmatched, and by which one, is a property of the DAG rather than of this decision - see
/// `MergedPlanDAG::getNearestPaddedSide` - and it applies to a column the lazy read returns just as much as to a
/// recomputed one.
enum class Placement : uint8_t
{
    /// Not involved: nothing below or above the `LIMIT` reads it. This is what `LazyFrontier::at` returns for a value with
    /// no entry, and what an entry starts as when it is created, so that `Below` and the others are set explicitly. A
    /// stored entry is never left as `None`.
    None,
    Below,      /// computed below the `LIMIT` only, for a filter, a join condition, the sort order or a carried value
    Carried,    /// computed below, and handed over as a column of the block the `LIMIT` cuts
    Deferred,   /// had above only: an input of a source is read lazily, anything else is computed there
    Recomputed, /// computed below, and again above: the case this is all for - a value a filter needs below the `LIMIT`
                /// and the result again above it
};

/// Whether the main branch computes the value.
inline bool isComputedBelow(Placement placement)
{
    return placement == Placement::Below || placement == Placement::Carried || placement == Placement::Recomputed;
}

/// Whether the branch above the `LIMIT` has the value of its own: computes it, or reads it lazily for an input.
inline bool isComputedAbove(Placement placement)
{
    return placement == Placement::Deferred || placement == Placement::Recomputed;
}

/// Where each value of the subtree gets computed.
///
/// The currency is columns crossing the plan below the `LIMIT`, not arithmetic: a crossing column is read
/// for every scanned row, goes through the sort, and a join replicates it, while recomputing a value
/// above the `LIMIT` touches at most `limit` rows. So a deferred value is recomputed from whatever is
/// already at hand - a column the main branch computes anyway, or a source column the lazy read
/// produces - and only a value that cannot be recomputed crosses.
struct LazyFrontier
{
    /// Values with no entry are `Placement::None`.
    std::unordered_map<const ActionsDAG::Node *, Placement> placement;

    Placement at(const ActionsDAG::Node * node) const;

    /// Whether anything is computed above the `LIMIT` at all. When false the caller has nothing to gain.
    bool defersAnything() const;
};

/// `eager_output_positions` are the outputs of `merged` a caller needs below the `LIMIT` anyway, i.e. the
/// sort description. Filters, join conditions and the values steps read are taken from `merged` itself.
/// `lazy_sources` says which sources support a second, row-addressed read; a value reading one that does
/// not is computed below the `LIMIT` and crosses instead.
///
/// `masked_padded_sides` are the padded sides the caller has a mask for above the `LIMIT`. A value gated by one
/// of them is recomputed there as `if(mask, value, default)`, which evaluates it on the rows the join
/// padded as well - so only a value whose function cannot throw (`IExecutableFunction::canThrow`) is
/// recomputed this way. Any other gated value, and one whose padded side has no mask, is computed below the
/// `LIMIT`, where the join pads it itself.
LazyFrontier chooseLazyFrontier(
    const MergedPlanDAG & merged,
    const std::vector<size_t> & eager_output_positions,
    const std::vector<bool> & lazy_sources,
    const std::unordered_set<const MergedPlanDAG::PaddedSide *> & masked_padded_sides = {});

}
