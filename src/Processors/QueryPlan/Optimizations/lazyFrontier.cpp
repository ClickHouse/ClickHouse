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
        frontier.recomputed_under_mask.resize(merged.sources.size());
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

    /// Tries to compute `output` above the `LIMIT`. Returns false when it cannot be, leaving the
    /// frontier as it was, so the caller can keep that output eager instead.
    bool tryDefer(const ActionsDAG::Node * output)
    {
        LazyFrontier candidate = frontier;
        if (!deferNode(output, candidate))
            return false;

        frontier = std::move(candidate);
        return true;
    }

    LazyFrontier takeFrontier() { return std::move(frontier); }

private:
    /// A source column is read again by the lazy read; anything else is recomputed from its children,
    /// which is where the leaked intermediate results disappear. A value the main branch computes anyway
    /// may instead be taken as it is, but only where that is the cheaper of the two.
    bool deferNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        if (candidate.carried.contains(node) || isPlaced(node, candidate))
            return true;

        if (candidate.eager.contains(node))
        {
            if (free_to_carry.contains(node))
            {
                candidate.carried.insert(node);
                return true;
            }

            /// This value was already used below the `LIMIT` and the result has to agree with it, so one
            /// that does not answer the same twice crosses as a column instead. What is computed from it
            /// may still be recomputed above, since that gives the same answer.
            if (!canBeRecomputed(node))
            {
                candidate.carried.insert(node);
                return true;
            }

            /// Recomputing costs nothing below the `LIMIT` and at most `limit` rows above it, while a
            /// carried column is read for every scanned row and replicated by every join on the way up.
            /// So recompute, unless doing so drags in more than one column nothing else reads - then one
            /// carried column is the smaller price.
            LazyFrontier attempt = candidate;
            if (recomputeNode(node, attempt) && countLazyReads(attempt) <= countLazyReads(candidate) + 1)
            {
                candidate = std::move(attempt);
                return true;
            }

            candidate.carried.insert(node);
            return true;
        }

        return recomputeNode(node, candidate);
    }

    bool recomputeNode(const ActionsDAG::Node * node, LazyFrontier & candidate)
    {
        if (const auto source = findSourceOfInput(merged, node))
        {
            if (!lazy_sources[*source])
                return false;

            candidate.lazily_read_inputs[*source].insert(node);
            return true;
        }

        /// An input reading no source, or more than one, is not something this can place.
        if (node->type == ActionsDAG::ActionType::INPUT)
            return false;

        /// Nothing below the `LIMIT` used this value, so computing it above is its first and only
        /// evaluation, and a non-deterministic function is free to answer whatever it answers. An
        /// `arrayJoin` is still out: it changes the number of rows the `LIMIT` already counted.
        if (node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;

        for (const auto * child : node->children)
            if (!deferNode(child, candidate))
                return false;

        /// Where a join can leave this node's source unmatched, the node has a value of its own only on
        /// the rows that matched. Everything it reads is available above the `LIMIT` all the same, carried
        /// columns included, so the placement is the same one, restricted by that source's mask.
        if (const auto masking_source = merged.getMaskingSource(node))
        {
            candidate.recomputed_under_mask[*masking_source].insert(node);
            return true;
        }

        candidate.recomputed_after_merge.insert(node);
        return true;
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

    ActionsDAG::NodeRawConstPtrs base_eager_roots = merged.filter_nodes;
    base_eager_roots.append_range(merged.join_condition_nodes);
    for (size_t position : eager_output_positions)
        base_eager_roots.push_back(outputs[position]);

    /// An output that cannot be deferred is computed below the `LIMIT` after all, which makes the values
    /// behind it available to carry - so an output that failed for want of one of them can be deferred on
    /// the next round. Repeat while that keeps happening; the eager set only grows, so it settles.
    std::vector<bool> is_eager_output(outputs.size(), false);
    while (true)
    {
        FrontierChooser chooser(merged, lazy_sources);

        auto eager_roots = base_eager_roots;
        for (size_t position = 0; position < outputs.size(); ++position)
            if (is_eager_output[position])
                eager_roots.push_back(outputs[position]);

        chooser.markEager(eager_roots);

        /// The sort keys and any output that stayed eager are handed over by the main branch in any case.
        ActionsDAG::NodeRawConstPtrs free_to_carry;
        for (size_t position : eager_output_positions)
            free_to_carry.push_back(outputs[position]);
        for (size_t position = 0; position < outputs.size(); ++position)
            if (is_eager_output[position])
                free_to_carry.push_back(outputs[position]);
        chooser.markFreeToCarry(free_to_carry);

        bool found_new_eager_output = false;
        for (size_t position = 0; position < outputs.size(); ++position)
        {
            if (is_eager_output[position])
                continue;

            if (!chooser.tryDefer(outputs[position]))
            {
                is_eager_output[position] = true;
                found_new_eager_output = true;
            }
        }

        if (found_new_eager_output)
            continue;

        auto frontier = chooser.takeFrontier();
        for (size_t position = 0; position < outputs.size(); ++position)
            if (is_eager_output[position])
                frontier.eager_outputs.push_back(position);

        return frontier;
    }
}

}
