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
    return std::ranges::any_of(
        placement, [](const auto & entry) { return entry.second.above != Placement::Above::No; });
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
        {
            auto & below = candidate.placement[node].below;
            if (below == Placement::Below::No)
                below = Placement::Below::Computed;
        }
    }

    void placeNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        const auto placed = candidate.at(node);
        if (placed.below == Placement::Below::ComputedAndCrossing || placed.above != Placement::Above::No)
            return;

        if (placed.below == Placement::Below::Computed)
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

            /// Recomputing costs nothing below the `LIMIT` and at most `limit` rows above it, while a
            /// crossing column is read for every scanned row and replicated by every join on the way up.
            /// So recompute, unless doing so drags in more than one column nothing else reads - then one
            /// crossing column is the smaller price.
            LazyFrontier attempt = candidate;
            if (recomputeNode(node, attempt) && countLazyReads(attempt) <= countLazyReads(candidate) + 1)
            {
                candidate = std::move(attempt);
                return;
            }

            cross(node, candidate);
            return;
        }

        if (!recomputeNode(node, candidate))
            cross(node, candidate);
    }

    /// Returns false when the value cannot be had above the `LIMIT` at all, leaving `candidate` in
    /// whatever state it reached - the caller either lets it cross or drops the attempt.
    bool recomputeNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        if (const auto source = findSourceOfInput(merged, node))
        {
            /// Without a second read of that source there is no way to get the column up here.
            if (!lazy_sources[*source])
                return false;

            candidate.placement[node].above = Placement::Above::LazyRead;
            return true;
        }

        /// An input reading no source, or more than one, is not something this can place.
        if (node->type == ActionsDAG::ActionType::INPUT)
            return false;

        /// An `arrayJoin` changes the number of rows the `LIMIT` already counted.
        if (node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;

        /// A child that has to cross is computed below the `LIMIT`, which `cross` takes care of.
        for (const auto * child : node->children)
            placeNode(child, candidate);

        candidate.placement[node].above = Placement::Above::Recomputed;
        return true;
    }

    /// A crossing value is computed below the `LIMIT`, so everything it reads is computed there too.
    static void cross(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        markComputedBelow({node}, candidate);
        candidate.placement[node].below = Placement::Below::ComputedAndCrossing;
    }

    static size_t countLazyReads(const LazyFrontier & candidate)
    {
        return std::ranges::count_if(
            candidate.placement, [](const auto & entry) { return entry.second.above == Placement::Above::LazyRead; });
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
