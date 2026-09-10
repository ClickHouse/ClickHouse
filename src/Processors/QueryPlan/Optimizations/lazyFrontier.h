#pragma once

#include <Processors/QueryPlan/Optimizations/mergedPlanDAG.h>

namespace DB::QueryPlanOptimizations
{

/// Where each column of a `Limit -> ... -> sources` subtree gets computed once the heavy columns are
/// deferred past the `LIMIT`.
///
/// The currency is columns crossing the plan below the `LIMIT`, not arithmetic: a carried column is read
/// for every scanned row, goes through the sort, and a join replicates it, while recomputing a value
/// above the `LIMIT` touches at most `limit` rows. So a deferred value is recomputed from whatever is
/// already at hand - a column the main branch computes anyway, or a source column the lazy read
/// produces - and only a value that cannot be recomputed is carried across.
struct LazyFrontier
{
    /// Computed below the `LIMIT`, because a filter, a join condition or the sort order needs it, or
    /// because it is carried and so has to be computed there.
    NodeSet eager;

    /// Computed above the `LIMIT`, from the frontier below, on all rows the plan produced there - stuffed
    /// ones included, which is what such a node saw before.
    NodeSet recomputed_after_merge;
    /// Computed above the `LIMIT` as well, but only on the rows where the join that gates it matched, the
    /// value that join stuffed standing everywhere else. Indexed by `MergedPlanDAG::stuffings`.
    ///
    /// The mask is free: any `Nullable` column from the unmatched side of that join - a source's row
    /// index, or a `toNullable` marker - is NULL at exactly the unmatched rows, whatever
    /// `join_use_nulls` says, and it is stuffed again by the joins above it, so it answers for those too.
    /// The rows have to be masked out rather than computed and discarded, because `intDiv(1, x)` over a
    /// stuffed `x = 0` throws whether or not its answer is used.
    std::vector<NodeSet> recomputed_under_mask;

    /// Values that cross the `LIMIT` as columns, because they are needed above it and cannot be
    /// recomputed there: a non-deterministic or stateful value the filter already used, and anything
    /// reading a source that has no second read.
    NodeSet carried;

    /// Per source, the input nodes its lazy read has to produce.
    std::vector<NodeSet> lazily_read_inputs;

    /// Whether anything is deferred at all. When false the caller has nothing to gain.
    bool defersAnything() const;
};

/// `eager_output_positions` are the outputs of `merged` a caller needs below the `LIMIT` anyway, i.e. the
/// sort description. Filters and join conditions are taken from `merged` itself.
/// `lazy_sources` says which sources support a second, row-addressed read; a value reading one that does
/// not is carried instead.
LazyFrontier chooseLazyFrontier(
    const MergedPlanDAG & merged,
    const std::vector<size_t> & eager_output_positions,
    const std::vector<bool> & lazy_sources);

}
