#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Block.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/checkStackSize.h>

namespace DB
{

namespace QueryPlanOptimizations
{

namespace
{

/// Prunes the columns at `unneeded_outputs` of the output header of `node`, its children before itself,
/// and says what it dropped. Going down, a step is only asked what it does not need of its children; it
/// changes on the way back up, once its children have changed and it knows what they really produce.
IQueryPlanStep::PrunedInput pruneNode(QueryPlan::Node & node, const std::vector<size_t> & unneeded_outputs, bool & changed)
{
    checkStackSize();

    auto & step = *node.step;

    /// A step that cannot drop columns needs all its children produce, and outputs all it did. So does a
    /// sink, which has no output at all, such as the root of a distributed plan fragment.
    const bool can_prune = step.hasOutputHeader() && step.canRemoveUnusedColumns();
    if (!can_prune)
    {
        for (size_t child = 0; child < node.children.size(); ++child)
        {
            const auto pruned = pruneNode(*node.children[child], {}, changed);

            /// Asked for everything, a child keeps everything, but the representation of a column may change,
            /// such as a constant that is materialized now.
            if (!blocksHaveEqualStructure(*step.getInputHeaders()[child], *pruned.header))
                step.updateInputHeader(pruned.header, child);
        }

        if (!step.hasOutputHeader())
            return {};

        return IQueryPlanStep::PrunedInput::unchanged(step.getOutputHeader());
    }

    std::vector<IQueryPlanStep::PrunedInput> children;
    if (!node.children.empty())
    {
        const auto unneeded = step.getUnneededColumns(unneeded_outputs);
        for (size_t child = 0; child < node.children.size(); ++child)
            children.push_back(pruneNode(*node.children[child], unneeded.at(child), changed));
    }

    auto result = step.removeUnusedColumns(unneeded_outputs, children);
    changed |= result.step_changed;
    return {std::move(result.dropped_output_positions), step.getOutputHeader()};
}

}

bool removeUnusedColumns(QueryPlan::Node & root)
{
    bool changed = false;
    pruneNode(root, {}, changed);
    return changed;
}

}
}
