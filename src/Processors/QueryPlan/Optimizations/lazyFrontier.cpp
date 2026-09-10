#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>

#include <Functions/IFunction.h>

namespace DB::QueryPlanOptimizations
{

bool LazyFrontier::defersAnything() const
{
    if (!recomputed_after_merge.empty())
        return true;

    return std::ranges::any_of(recomputed_under_mask, [](const auto & nodes) { return !nodes.empty(); });
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
        frontier.recomputed_under_mask.resize(merged.stuffings.size());
        frontier.lazily_read_inputs.resize(merged.sources.size());
    }

    /// Everything a filter, a join condition or the sort order needs is computed below the `LIMIT`,
    /// together with everything those values are computed from.
    void markEager(const ActionsDAG::NodeRawConstPtrs & roots)
    {
        for (const auto * node : findReachableNodes(roots))
            frontier.eager.insert(node);
    }

    /// Values the main branch has to hand over anyway, the sort keys above all: they are part of the block
    /// the `LIMIT` produces whatever this decides, so taking them costs nothing.
    void markFreeToCarry(const ActionsDAG::NodeRawConstPtrs & nodes)
    {
        free_to_carry.insert(nodes.begin(), nodes.end());
    }

    /// Places `node` and everything it reads. Every node can be placed: what cannot be recomputed above
    /// the `LIMIT` is computed below it and carried, which is what the plan did anyway.
    void place(const ActionsDAG::Node * node) { placeNode(node, frontier); }

    LazyFrontier takeFrontier() { return std::move(frontier); }

private:
    void placeNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        if (candidate.carried.contains(node) || isPlaced(node, candidate))
            return;

        if (candidate.eager.contains(node))
        {
            /// The main branch hands this one over regardless, so there is nothing to weigh up.
            if (free_to_carry.contains(node))
            {
                carry(node, candidate);
                return;
            }

            /// This value was already used below the `LIMIT` and the result has to agree with it, so one
            /// that does not answer the same twice crosses as a column instead. What is computed from it
            /// may still be recomputed above, since that gives the same answer.
            if (!canBeRecomputed(node))
            {
                carry(node, candidate);
                return;
            }

            /// Recomputing costs nothing below the `LIMIT` and at most `limit` rows above it, while a
            /// carried column is read for every scanned row and replicated by every join on the way up.
            /// So recompute, unless doing so drags in more than one column nothing else reads - then one
            /// carried column is the smaller price.
            LazyFrontier attempt = candidate;
            if (recomputeNode(node, attempt) && countLazyReads(attempt) <= countLazyReads(candidate) + 1)
            {
                candidate = std::move(attempt);
                return;
            }

            carry(node, candidate);
            return;
        }

        if (!recomputeNode(node, candidate))
            carry(node, candidate);
    }

    /// Returns false when the node cannot be computed above the `LIMIT` at all, leaving `candidate` in
    /// whatever state it reached - the caller either carries the node or drops the attempt.
    bool recomputeNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        if (const auto source = findSourceOfInput(merged, node))
        {
            /// Without a second read of that source there is no way to get the column up here.
            if (!lazy_sources[*source])
                return false;

            candidate.lazily_read_inputs[*source].insert(node);
            return placeMasked(node, candidate);
        }

        /// An input reading no source, or more than one, is not something this can place.
        if (node->type == ActionsDAG::ActionType::INPUT)
            return false;

        /// An `arrayJoin` changes the number of rows the `LIMIT` already counted.
        if (node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;

        for (const auto * child : node->children)
        {
            placeNode(child, candidate);
            /// A child that had to be carried has to be computed below the `LIMIT`, which is where it
            /// already is: `carry` puts it and everything it reads into the eager set.
        }

        return placeMasked(node, candidate);
    }

    /// Where a join can leave this node's side unmatched, the node has a value of its own only on the
    /// rows where that join matched; the value it stuffed stands everywhere else. Everything the node
    /// reads is available above the `LIMIT` all the same, carried columns included, so this is the same
    /// placement, restricted by that join's mask.
    bool placeMasked(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        if (const auto stuffing = merged.getNearestStuffing(node))
            candidate.recomputed_under_mask[*stuffing].insert(node);
        else
            candidate.recomputed_after_merge.insert(node);

        return true;
    }

    /// A carried value is computed below the `LIMIT`, so everything it reads is computed there too.
    void carry(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        candidate.carried.insert(node);
        for (const auto * needed : findReachableNodes({node}))
            candidate.eager.insert(needed);
    }

    static bool isPlaced(const ActionsDAG::Node * node, const LazyFrontier & candidate)
    {
        if (candidate.recomputed_after_merge.contains(node))
            return true;

        return std::ranges::any_of(
            candidate.recomputed_under_mask, [&](const auto & nodes) { return nodes.contains(node); });
    }

    static size_t countLazyReads(const LazyFrontier & candidate)
    {
        size_t count = 0;
        for (const auto & inputs : candidate.lazily_read_inputs)
            count += inputs.size();
        return count;
    }

    const MergedPlanDAG & merged;
    const std::vector<bool> & lazy_sources;
    NodeSet free_to_carry;
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

    ActionsDAG::NodeRawConstPtrs eager_roots = merged.filter_nodes;
    eager_roots.append_range(merged.join_condition_nodes);

    ActionsDAG::NodeRawConstPtrs free_to_carry;
    for (size_t position : eager_output_positions)
    {
        eager_roots.push_back(outputs[position]);
        free_to_carry.push_back(outputs[position]);
    }

    chooser.markEager(eager_roots);
    chooser.markFreeToCarry(free_to_carry);

    for (const auto * output : outputs)
        chooser.place(output);

    return chooser.takeFrontier();
}

}
