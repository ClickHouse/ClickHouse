#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Block.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <stack>

namespace DB
{

namespace QueryPlanOptimizations
{

namespace
{

/// A step on the way down, with what the step above does not need of it and what its children produce once
/// they are pruned themselves.
struct PruningFrame
{
    QueryPlan::Node * node = nullptr;
    std::vector<size_t> unneeded_outputs;
    size_t depth = 0;

    /// A step that cannot drop columns needs all its children produce, and outputs all it did. So does a
    /// sink, which has no output at all, such as the root of a distributed plan fragment.
    bool can_prune = false;
    IQueryPlanStep::UnneededInputPositions unneeded_inputs;

    std::vector<IQueryPlanStep::PrunedInput> children;
};

PruningFrame makeFrame(QueryPlan::Node & node, std::vector<size_t> unneeded_outputs, size_t depth)
{
    PruningFrame frame;
    frame.node = &node;
    frame.unneeded_outputs = std::move(unneeded_outputs);
    frame.depth = depth;
    const auto & step = *node.step;
    frame.can_prune = step.hasOutputHeader() && step.canRemoveUnusedColumns();

    /// Going down, a step is only asked what it does not need of its children; it changes on the way back up,
    /// once its children have changed and it knows what they really produce.
    if (frame.can_prune && !node.children.empty())
        frame.unneeded_inputs = step.getUnneededColumns(frame.unneeded_outputs);

    return frame;
}

/// Prunes the frame's step once its children are done, and says what it dropped.
IQueryPlanStep::PrunedInput pruneStep(PruningFrame & frame, bool & changed)
{
    auto & step = *frame.node->step;

    if (!frame.can_prune)
    {
        /// Asked for everything, a child keeps everything, but the representation of a column may change, such as a
        /// constant that is materialized now.
        for (size_t child = 0; child < frame.children.size(); ++child)
            if (!blocksHaveEqualStructure(*step.getInputHeaders()[child], *frame.children[child].header))
                step.updateInputHeader(frame.children[child].header, child);

        if (!step.hasOutputHeader())
            return {};

        return IQueryPlanStep::PrunedInput::unchanged(step.getOutputHeader());
    }

    auto result = step.removeUnusedColumns(frame.unneeded_outputs, frame.children);
    changed = result.step_changed;
    return {std::move(result.dropped_output_positions), step.getOutputHeader()};
}

}

size_t removeUnusedColumns(QueryPlan::Node & root, RemoveUnusedColumnsMode mode)
{
    const bool is_local = mode == RemoveUnusedColumnsMode::Local;

    /// The root keeps all its outputs: nothing above it is looked at.
    std::stack<PruningFrame> stack;
    stack.push(makeFrame(root, {}, 0));
    if (is_local && !stack.top().can_prune)
        return 0;

    size_t changed_depth = 0;
    while (!stack.empty())
    {
        /// A reference stays valid while frames are pushed above it: the stack is a `std::deque`.
        auto & frame = stack.top();
        const size_t child = frame.children.size();
        if (child < frame.node->children.size())
        {
            auto & child_node = *frame.node->children[child];

            /// A step that cannot drop columns keeps its children as they are. So does a local walk where nothing of
            /// a child is unneeded any more: what is left there is the business of the child itself.
            if (frame.can_prune)
            {
                auto & unneeded = frame.unneeded_inputs.at(child);
                if (!is_local || !unneeded.empty())
                {
                    stack.push(makeFrame(child_node, std::move(unneeded), frame.depth + 1));
                    if (!is_local || stack.top().can_prune)
                        continue;

                    /// A child that cannot drop columns keeps everything it was asked not to.
                    stack.pop();
                }
            }
            else if (!is_local)
            {
                stack.push(makeFrame(child_node, {}, frame.depth + 1));
                continue;
            }

            frame.children.push_back(IQueryPlanStep::PrunedInput::unchanged(child_node.step->getOutputHeader()));
            continue;
        }

        bool changed = false;
        auto pruned = pruneStep(frame, changed);
        if (changed)
            changed_depth = std::max(changed_depth, frame.depth + 1);

        stack.pop();
        if (!stack.empty())
            stack.top().children.push_back(std::move(pruned));
    }

    return changed_depth;
}

size_t tryRemoveUnusedColumns(QueryPlan::Node * node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
    return removeUnusedColumns(*node, RemoveUnusedColumnsMode::Local);
}

}
}
