#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{
namespace QueryPlanOptimizations
{

bool canAllChildrenCanRemoveOutputs(const QueryPlan::Node & node)
{
    return std::all_of(
        node.children.begin(),
        node.children.end(),
        [](const QueryPlan::Node * child) { return child->step->canRemoveUnusedColumns() && child->step->canRemoveColumnsFromOutput(); });
}

bool removeChildrenOutputs(const QueryPlan::Node & node)
{
    bool updated = false;
    const auto & parent_inputs = node.step->getInputHeaders();

    for (auto child_id = 0U; child_id < node.children.size(); ++child_id)
    {
        auto & child_step = node.children[child_id]->step;
        chassert(child_step->canRemoveUnusedColumns());
        const auto & required_outputs = parent_inputs[child_id];

        // Here we never want to remove inputs because the grandchildren might cannot remove outputs`
        const auto updated_anything = child_step->removeUnusedColumns(required_outputs.getNames(), false).updated_anything;

        // TODO(antaljanosbenjamin): compare the header with name and types
        if (updated_anything || !blocksHaveEqualStructure(child_step->getOutputHeader(), parent_inputs[child_id]))
            node.step->updateInputHeader(child_step->getOutputHeader(), child_id);

        if (updated_anything)
            updated = true;
    }

    return updated;
}

size_t tryRemoveUnusedColumns(QueryPlan::Node * node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
    auto & parent = node->step;

    if (parent->getOutputHeader().columns() == 0)
        return 0;

    bool updated_child = false;
    bool updated_grandchild = false;

    const auto & parent_inputs = parent->getInputHeaders();

    for (auto child_id = 0U; child_id < node->children.size(); ++child_id)
    {
        auto * child_node = node->children[child_id];
        auto & child_step = child_node->step;
        if (!child_step->canRemoveUnusedColumns())
            continue;

        const auto can_remove_inputs = canAllChildrenCanRemoveOutputs(*child_node);

        const auto & required_outputs = parent_inputs[child_id];
        const auto remove_result = child_step->removeUnusedColumns(required_outputs.getNames(), can_remove_inputs);

        if (remove_result.updated_anything)
        {
            updated_child = true;

            if (remove_result.removed_any_input)
                updated_grandchild |= removeChildrenOutputs(*child_node);

            parent->updateInputHeader(child_step->getOutputHeader(), child_id);
        }
    }

    if (updated_grandchild)
        return 3;

    if (updated_child)
        return 2;

    return 0;
}

}
}
