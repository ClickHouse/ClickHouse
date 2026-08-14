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
    /// Computed below the `LIMIT`, because a filter, a join condition or the sort order needs it.
    NodeSet eager;

    /// Computed above the `LIMIT`, from the frontier below, on all rows the plan produced there - stuffed
    /// ones included, which is what such a node saw before.
    NodeSet recomputed_after_merge;
    /// Computed above the `LIMIT` as well, but only on the rows where one source matched, the value the
    /// join stuffed standing everywhere else. Needed for a node the plan computed below a join that can
    /// leave that source unmatched: there the join replaced the node's value by a default or a NULL, so
    /// running it on those rows would turn `x + 1` over a stuffed 0 into 1 rather than the default 0.
    /// Indexed by source.
    ///
    /// The mask is free: any `Nullable` column from that side of the join - the source's row index, or a
    /// `toNullable` marker where the source is not read lazily - is NULL at exactly the unmatched rows,
    /// whatever `join_use_nulls` says. The rows must be masked out rather than computed and discarded,
    /// because `intDiv(1, x)` over a stuffed `x = 0` throws whether or not its answer is used.
    std::vector<NodeSet> recomputed_under_mask;

    /// Nodes that have to cross the `LIMIT` as columns: they are needed above it but cannot be
    /// recomputed there, because a non-deterministic or stateful function would not give the same answer
    /// twice, or because they are only available in the main branch.
    NodeSet carried;

    /// Per source, the input nodes its lazy read has to produce.
    std::vector<NodeSet> lazily_read_inputs;

    /// Output positions that stayed eager after all, because deferring them turned out to need a source
    /// that cannot be read lazily, or a value that can be neither carried nor recomputed.
    std::vector<size_t> eager_outputs;

    /// Whether anything is deferred at all. When false the caller has nothing to gain.
    bool defersAnything() const;
};

/// `eager_output_positions` are the outputs of `merged` a caller needs below the `LIMIT` anyway, i.e. the
/// sort description. Filters and join conditions are taken from `merged` itself.
/// `lazy_sources` says which sources support a second, row-addressed read; the rest stay eager.
LazyFrontier chooseLazyFrontier(
    const MergedPlanDAG & merged,
    const std::vector<size_t> & eager_output_positions,
    const std::vector<bool> & lazy_sources);

}
