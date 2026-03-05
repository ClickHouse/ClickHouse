#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/SortDescription.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>


namespace DB::QueryPlanOptimizations
{

namespace
{

const ActionsDAG::Node * dealiasNode(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

/// Information about a single volume-reducing function output in the parent's DAG.
struct FunctionInfo
{
    /// The output node in the parent's DAG (may be an ALIAS wrapping the function).
    const ActionsDAG::Node * output_node;
    /// The dealiased FUNCTION node.
    const ActionsDAG::Node * function_node;
};

/// A source column that is only consumed by volume-reducing functions and can be pushed down.
struct PushdownCandidate
{
    const ActionsDAG::Node * source_input_node;
    std::vector<FunctionInfo> functions;
};

/// Check whether a column is needed by a step's own operation (sort key, distinct key, etc.).
/// Returns true if the step needs this column — meaning we cannot remove it from the data stream.
bool isColumnUsedByStep(const IQueryPlanStep * step, const std::string & column_name)
{
    if (const auto * sorting = typeid_cast<const SortingStep *>(step))
    {
        for (const auto & col : sorting->getSortDescription())
            if (col.column_name == column_name)
                return true;
        return false;
    }

    if (typeid_cast<const LimitStep *>(step))
        return false;

    if (const auto * distinct = typeid_cast<const DistinctStep *>(step))
    {
        for (const auto & name : distinct->getColumnNames())
            if (name == column_name)
                return true;
        return false;
    }

    if (const auto * expr = typeid_cast<const ExpressionStep *>(step))
    {
        /// Check if any FUNCTION node in the child's DAG uses this column as input.
        /// Pass-through (INPUT → OUTPUT) does not count as "used".
        for (const auto & node : expr->getExpression().getNodes())
        {
            if (node.type != ActionsDAG::ActionType::FUNCTION)
                continue;
            for (const auto * child : node.children)
                if (dealiasNode(child)->type == ActionsDAG::ActionType::INPUT && dealiasNode(child)->result_name == column_name)
                    return true;
        }
        return false;
    }

    if (const auto * filter = typeid_cast<const FilterStep *>(step))
    {
        for (const auto & node : filter->getExpression().getNodes())
        {
            if (node.type != ActionsDAG::ActionType::FUNCTION)
                continue;
            for (const auto * child : node.children)
                if (dealiasNode(child)->type == ActionsDAG::ActionType::INPUT && dealiasNode(child)->result_name == column_name)
                    return true;
        }
        return false;
    }

    /// Unknown step type — conservatively assume the column is used.
    return true;
}

/// For ExpressionStep / FilterStep children whose DAG passes the source column through,
/// update the DAG: remove the old pass-through and add new pass-throughs for the function results.
void updateChildDAGForPushedColumns(
    IQueryPlanStep * step,
    const std::unordered_map<std::string, PushdownCandidate> & candidates)
{
    ActionsDAG * child_dag = nullptr;
    if (auto * expr = typeid_cast<ExpressionStep *>(step))
        child_dag = &expr->getExpression();
    else if (auto * filter = typeid_cast<FilterStep *>(step))
        child_dag = &filter->getExpression();

    if (!child_dag)
        return;

    for (const auto & [source_name, cand] : candidates)
    {
        /// Remove the source column from outputs.
        auto & outputs = child_dag->getOutputs();
        std::erase_if(outputs, [&](const ActionsDAG::Node * node)
        {
            const auto * actual = dealiasNode(node);
            return actual->type == ActionsDAG::ActionType::INPUT && actual->result_name == source_name;
        });

        /// Add pass-through INPUT nodes for each function result.
        for (const auto & func : cand.functions)
        {
            const auto & new_input = child_dag->addInput(func.output_node->result_name, func.output_node->result_type);
            child_dag->addOrReplaceInOutputs(new_input);
        }
    }

    child_dag->removeUnusedActions();
}

} // anonymous namespace


size_t tryPushDownVolumeReducingFunctions(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    /// --- 1. Validate parent --------------------------------------------------------

    if (parent_node->children.size() != 1)
        return 0;

    auto * parent_expr = typeid_cast<ExpressionStep *>(parent_node->step.get());
    auto * parent_filter = typeid_cast<FilterStep *>(parent_node->step.get());
    if (!parent_expr && !parent_filter)
        return 0;

    auto & parent_dag = parent_expr ? parent_expr->getExpression() : parent_filter->getExpression();

    if (parent_dag.hasArrayJoin())
        return 0;

    /// --- 2. Find volume-reducing function outputs ----------------------------------

    /// Map: source column name -> pushdown candidate.
    std::unordered_map<std::string, PushdownCandidate> candidates;

    for (const auto * output : parent_dag.getOutputs())
    {
        const auto * func_node = dealiasNode(output);
        if (func_node->type != ActionsDAG::ActionType::FUNCTION)
            continue;
        if (!func_node->function_base || !func_node->function_base->isSuitableForPushDownBeforeFilter())
            continue;

        /// The function must have exactly one non-constant INPUT child.
        const ActionsDAG::Node * source_input = nullptr;
        bool valid = true;
        for (const auto * child : func_node->children)
        {
            const auto * actual = dealiasNode(child);
            if (actual->type == ActionsDAG::ActionType::INPUT)
            {
                if (source_input && source_input != actual)
                {
                    valid = false;
                    break;
                }
                source_input = actual;
            }
            else if (actual->type != ActionsDAG::ActionType::COLUMN)
            {
                /// Non-trivial sub-expression — skip.
                valid = false;
                break;
            }
        }
        if (!valid || !source_input)
            continue;

        auto & cand = candidates[source_input->result_name];
        cand.source_input_node = source_input;
        cand.functions.push_back({output, func_node});
    }

    if (candidates.empty())
        return 0;

    /// --- 3. Verify source columns are ONLY used by volume-reducing functions -------

    /// Collect all VR function nodes so we can skip them when scanning for other uses.
    std::unordered_set<const ActionsDAG::Node *> vr_func_nodes;
    for (const auto & [name, cand] : candidates)
        for (const auto & func : cand.functions)
            vr_func_nodes.insert(func.function_node);

    /// If ANY non-VR function in the DAG uses a candidate source column -> exclude it.
    for (const auto & node : parent_dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || vr_func_nodes.contains(&node))
            continue;
        for (const auto * child : node.children)
        {
            const auto * actual = dealiasNode(child);
            if (actual->type == ActionsDAG::ActionType::INPUT)
                candidates.erase(actual->result_name);
        }
    }

    /// If a source column appears directly in the parent's outputs -> exclude it.
    for (const auto * output : parent_dag.getOutputs())
    {
        const auto * actual = dealiasNode(output);
        if (actual->type == ActionsDAG::ActionType::INPUT)
            candidates.erase(actual->result_name);
    }

    if (candidates.empty())
        return 0;

    /// --- 4. Check child step compatibility -----------------------------------------

    auto * child_node = parent_node->children.front();
    auto & child_step = child_node->step;

    /// The child must have exactly one child itself (so we can insert a step below it).
    if (child_node->children.size() != 1)
        return 0;

    for (auto it = candidates.begin(); it != candidates.end(); )
    {
        if (isColumnUsedByStep(child_step.get(), it->first))
            it = candidates.erase(it);
        else
            ++it;
    }

    if (candidates.empty())
        return 0;

    /// Check that no pushed function result name collides with existing grandchild output,
    /// and that every source column actually exists in the grandchild output
    /// (the child step might compute the source column itself rather than passing it through).
    const auto & grandchild_output = child_node->children.front()->step->getOutputHeader();
    for (auto it = candidates.begin(); it != candidates.end(); )
    {
        if (!grandchild_output->has(it->first))
        {
            it = candidates.erase(it);
            continue;
        }
        bool has_collision = false;
        for (const auto & func : it->second.functions)
        {
            if (grandchild_output->has(func.output_node->result_name))
                has_collision = true;
        }
        if (has_collision)
            return 0;
        ++it;
    }

    if (candidates.empty())
        return 0;

    /// --- 5. Build new ActionsDAG for the inserted ExpressionStep --------------------

    ActionsDAG new_dag;

    /// Add inputs for every column in the grandchild's output.
    std::unordered_map<std::string, const ActionsDAG::Node *> new_dag_inputs;
    for (const auto & col : *grandchild_output)
    {
        const auto & input = new_dag.addInput(col.name, col.type);
        new_dag_inputs[col.name] = &input;
    }

    /// For each candidate, add the volume-reducing functions and remove the source column.
    for (const auto & [source_name, cand] : candidates)
    {
        auto it_input = new_dag_inputs.find(source_name);
        chassert(it_input != new_dag_inputs.end());
        const auto * source_in_new_dag = it_input->second;

        for (const auto & func : cand.functions)
        {
            /// Reconstruct function children in the new DAG.
            ActionsDAG::NodeRawConstPtrs children;
            for (const auto * orig_child : func.function_node->children)
            {
                const auto * actual = dealiasNode(orig_child);
                if (actual->type == ActionsDAG::ActionType::INPUT)
                {
                    children.push_back(source_in_new_dag);
                }
                else if (actual->type == ActionsDAG::ActionType::COLUMN)
                {
                    const auto & const_node = new_dag.addColumn(
                        {actual->column, actual->result_type, actual->result_name});
                    children.push_back(&const_node);
                }
            }

            /// Use the parent output node's name so column names are consistent.
            const auto & new_func = new_dag.addFunction(
                func.function_node->function_base,
                std::move(children),
                func.output_node->result_name);
            new_dag.addOrReplaceInOutputs(new_func);
        }

        /// Remove the source column from outputs of the new DAG.
        auto & outputs = new_dag.getOutputs();
        std::erase_if(outputs, [&](const ActionsDAG::Node * node) { return node->result_name == source_name; });
    }

    /// --- 6. Insert the new ExpressionStep between child and grandchild --------------

    auto new_step = std::make_unique<ExpressionStep>(grandchild_output, std::move(new_dag));
    new_step->setStepDescription("Compute volume-reducing functions");

    auto & inserted_node = nodes.emplace_back();
    inserted_node.step = std::move(new_step);
    inserted_node.children.swap(child_node->children);
    child_node->children = {&inserted_node};

    /// --- 7. Update child step -------------------------------------------------------

    /// For DAG-based child steps, patch the DAG to reflect the column change.
    updateChildDAGForPushedColumns(child_step.get(), candidates);

    child_step->updateInputHeader(inserted_node.step->getOutputHeader());

    /// --- 8. Update parent DAG -------------------------------------------------------

    /// Replace each pushed function output with a simple INPUT node.
    for (const auto & [source_name, cand] : candidates)
    {
        for (const auto & func : cand.functions)
        {
            const auto & new_input = parent_dag.addInput(
                func.output_node->result_name,
                func.output_node->result_type);

            auto & p_outputs = parent_dag.getOutputs();
            for (auto & out : p_outputs)
            {
                if (out == func.output_node)
                    out = &new_input;
            }
        }
    }
    parent_dag.removeUnusedActions();

    /// --- 9. Update parent input header ----------------------------------------------

    parent_node->step->updateInputHeader(child_step->getOutputHeader());

    return 2;
}

}
