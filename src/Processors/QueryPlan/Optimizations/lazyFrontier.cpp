#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>

#include <Functions/IFunction.h>

namespace DB::QueryPlanOptimizations
{

Placement LazyFrontier::at(const ActionsDAG::Node * node) const
{
    if (const auto it = placement.find(node); it != placement.end())
        return it->second;

    return {};
}

bool LazyFrontier::defersAnything() const
{
    return std::ranges::any_of(placement, [](const auto & entry)
    {
        const auto above = entry.second.above;
        return above == Placement::Above::LazyRead || above == Placement::Above::Recomputed;
    });
}

namespace
{

/// A value that does not answer the same twice cannot be recomputed above the `LIMIT`: the filter below
/// already used one answer, and the result has to agree with it. Same for a function whose answer depends
/// on the rows around it, which above the `LIMIT` are no longer the same rows.
bool canBeRecomputed(const ActionsDAG::Node * node)
{
    if (node->type == ActionsDAG::ActionType::ARRAY_JOIN)
        return false;

    if (node->type == ActionsDAG::ActionType::COLUMN)
        return node->is_deterministic_constant;

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    return node->function_base->isDeterministicInScopeOfQuery() && !node->function_base->isStateful();
}

/// Which source an input belongs to, or nullopt for a node that is not an input.
std::optional<size_t> findSourceOfInput(const MergedPlanDAG & merged, const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};

    const auto & node_sources = merged.getSources(node);
    if (node_sources.count() != 1)
        return {};

    return *node_sources.begin();
}

class FrontierChooser
{
public:
    FrontierChooser(const MergedPlanDAG & merged_, const std::vector<bool> & lazy_sources_)
        : merged(merged_), lazy_sources(lazy_sources_)
    {
    }

    /// Everything a filter, a join condition or the sort order needs is computed below the `LIMIT`,
    /// together with everything those values are computed from.
    void markComputedBelow(const ActionsDAG::NodeRawConstPtrs & roots) { markComputedBelow(roots, frontier); }

    /// Values the main branch has to hand over anyway, the sort keys above all: they are part of the
    /// block the `LIMIT` produces whatever this decides, so taking them costs nothing.
    void markFreeToCross(const ActionsDAG::NodeRawConstPtrs & nodes) { free_to_cross.insert(nodes.begin(), nodes.end()); }

    /// Places `node` and everything it reads. Every value can be placed: what cannot be recomputed above
    /// the `LIMIT` is computed below it and crosses, which is what the plan did anyway.
    void place(const ActionsDAG::Node * node) { placeNode(node, frontier); }

    LazyFrontier takeFrontier() { return std::move(frontier); }

private:
    static void markComputedBelow(const ActionsDAG::NodeRawConstPtrs & roots, LazyFrontier & candidate)
    {
        for (const auto * node : findReachableNodes(roots))
            candidate.placement[node].computed_below = true;
    }

    void placeNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        const auto placed = candidate.at(node);
        if (placed.above != Placement::Above::No)
            return;

        if (placed.computed_below)
        {
            /// The main branch hands this one over regardless, so there is nothing to weigh up.
            if (free_to_cross.contains(node))
            {
                cross(node, candidate);
                return;
            }

            /// This value was already used below the `LIMIT` and the result has to agree with it, so one
            /// that does not answer the same twice crosses as a column instead. What is computed from it
            /// may still be recomputed above, since that gives the same answer.
            if (!canBeRecomputed(node))
            {
                cross(node, candidate);
                return;
            }

            if (!preferRecomputing(node) || !canBePlacedAbove(node))
            {
                cross(node, candidate);
                return;
            }

            recomputeNode(node, candidate);
            return;
        }

        if (!canBePlacedAbove(node))
        {
            cross(node, candidate);
            return;
        }

        recomputeNode(node, candidate);
    }

    /// Whether the value can be had above the `LIMIT` at all. A local question: what the value reads is
    /// placed on its own terms, and the worst those terms come to is a column that crosses, which is
    /// available above just the same. So this never depends on what is below it.
    bool canBePlacedAbove(const ActionsDAG::Node * node) const
    {
        /// Without a second read of its source there is no way to get a column up there.
        if (const auto source = findSourceOfInput(merged, node))
            return lazy_sources[*source];

        /// An input reading no source, or more than one, is not something this can place, and an
        /// `arrayJoin` changes the number of rows the `LIMIT` already counted.
        return node->type != ActionsDAG::ActionType::INPUT && node->type != ActionsDAG::ActionType::ARRAY_JOIN;
    }

    /// Whether to compute a value the main branch already computes a second time above the `LIMIT`,
    /// rather than have the main branch hand it over.
    ///
    /// A crossing column is replicated by every join above it, and a hash join copies it into its build
    /// side, over every row that reaches there; recomputing touches at most `limit` rows. Where a join
    /// sits above the value that trade is worth taking, and where none does the value is reused, since
    /// carrying it past a sort costs little and reading its inputs again costs something.
    ///
    /// Which side of a join becomes the build side is not known here - the joins are still logical, and
    /// `convertLogicalJoinToPhysical` and the `join_swap_table` swap both come later - so the answer
    /// cannot be sharpened by asking.
    ///
    /// What this does not weigh is how expensive the expression is. Recomputing a regular expression
    /// match over `limit` rows can cost more than carrying a column, and `limit` reaches
    /// `query_plan_max_limit_for_lazy_materialization`, so this is the place where a per-function
    /// estimate belongs once there is one to ask.
    bool preferRecomputing(const ActionsDAG::Node * node) const { return merged.hasJoinAbove(node); }

    /// Puts the value above the `LIMIT`. Only for a value `canBePlacedAbove` accepts.
    void recomputeNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        if (findSourceOfInput(merged, node))
        {
            candidate.placement[node].above = Placement::Above::LazyRead;
            return;
        }

        /// A child that has to cross is computed below the `LIMIT`, which `cross` takes care of.
        for (const auto * child : node->children)
            placeNode(child, candidate);

        candidate.placement[node].above = Placement::Above::Recomputed;
    }

    /// A crossing value is computed below the `LIMIT`, so everything it reads is computed there too.
    static void cross(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        markComputedBelow({node}, candidate);
        candidate.placement[node].above = Placement::Above::Crossing;
    }

    const MergedPlanDAG & merged;
    const std::vector<bool> & lazy_sources;
    NodeSet free_to_cross;
    LazyFrontier frontier;
};

}

LazyFrontier chooseLazyFrontier(
    const MergedPlanDAG & merged,
    const std::vector<size_t> & eager_output_positions,
    const std::vector<bool> & lazy_sources)
{
    chassert(lazy_sources.size() == merged.sources.size());

    const auto & outputs = merged.getOutputs();

    FrontierChooser chooser(merged, lazy_sources);

    ActionsDAG::NodeRawConstPtrs computed_below_roots = merged.filter_nodes;
    computed_below_roots.append_range(merged.join_condition_nodes);

    ActionsDAG::NodeRawConstPtrs free_to_cross;
    for (size_t position : eager_output_positions)
    {
        computed_below_roots.push_back(outputs[position]);
        free_to_cross.push_back(outputs[position]);
    }

    chooser.markComputedBelow(computed_below_roots);
    chooser.markFreeToCross(free_to_cross);

    for (const auto * output : outputs)
        chooser.place(output);

    return chooser.takeFrontier();
}

std::vector<NodeSet> collectLazyReads(const MergedPlanDAG & merged, const LazyFrontier & frontier)
{
    std::vector<NodeSet> reads(merged.sources.size());

    for (const auto & [node, placed] : frontier.placement)
    {
        if (placed.above != Placement::Above::LazyRead)
            continue;

        const auto & node_sources = merged.getSources(node);
        chassert(node_sources.count() == 1);
        reads[*node_sources.begin()].insert(node);
    }

    return reads;
}

}
