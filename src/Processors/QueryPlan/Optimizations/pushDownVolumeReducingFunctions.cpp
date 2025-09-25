#include <cstddef>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace QueryPlanOptimizations
{
namespace
{

extern inline const ActionsDAG::Node * dealiasNode(const ActionsDAG::Node * node);
struct ColumnCandidate;
struct PushdownRequest;

using NodeToParentsMap = std::unordered_map<QueryPlan::Node *, std::vector<QueryPlan::Node *>>;
using NodeToNameMap = std::unordered_map<QueryPlan::Node *, std::string>;
using ColumnCandidatePtr = std::shared_ptr<ColumnCandidate>;
using ColumnCandidatesStack = std::vector<ColumnCandidatePtr>;
using PushdownRequestPtr = std::shared_ptr<PushdownRequest>;
using PushdownRequestsStack = std::vector<PushdownRequestPtr>;

/// Represent a pushable function or a column that can be pushed down
struct ColumnCandidate
{
    /// This represents how far back functions using this node can be pushed down
    QueryPlan::Node * max_pushdown_point;
    NodeToNameMap node_to_input_name;
    NodeToNameMap node_to_output_name;

    [[maybe_unused]] ColumnCandidate(QueryPlan::Node * node, const std::string & input_name, const std::string & output_name)
        : max_pushdown_point(node)
        , node_to_input_name({{node, input_name}})
        , node_to_output_name({{node, output_name}})
    {
    }
};

/// Represents a request to push down a volume-reducing function
struct PushdownRequest
{
    QueryPlan::Node * requesting_node;
    // the input column candidate is the column that would be replaced by function results
    std::shared_ptr<ColumnCandidate> input_column_candidate;
    // the function column candidates are the functions which use the input column to produce the final output columns
    std::vector<std::shared_ptr<ColumnCandidate>> function_column_candidates;

    [[maybe_unused]] PushdownRequest(
        QueryPlan::Node * node,
        const ColumnCandidatePtr & input_candidate,
        const std::vector<std::shared_ptr<ColumnCandidate>> & function_candidates)
        : requesting_node(node)
        , input_column_candidate(input_candidate)
        , function_column_candidates(function_candidates)
    {
    }

    const ActionsDAG::Node * getFunctionNode(const ColumnCandidatePtr & function_candidate) const
    {
        std::string name = function_candidate->node_to_output_name[requesting_node];
        ExpressionStep * expression_step = typeid_cast<ExpressionStep *>(requesting_node->step.get());
        FilterStep * filter_step = typeid_cast<FilterStep *>(requesting_node->step.get());

        chassert(expression_step || filter_step);

        ActionsDAG & actions_dag = expression_step ? expression_step->getExpression() : filter_step->getExpression();

        for (const ActionsDAG::Node * child : actions_dag.getOutputs())
        {
            if (child->result_name == name)
                return dealiasNode(child);
        }

        return nullptr;
    }
};


inline const ActionsDAG::Node * dealiasNode(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

/// phase 1

NodeToParentsMap buildDirectParentNodesMap(QueryPlan::Node * root_node)
{
    NodeToParentsMap direct_parent_nodes_map;
    std::vector<QueryPlan::Node *> stack;
    stack.push_back(root_node);

    while (!stack.empty())
    {
        QueryPlan::Node * current_node = stack.back();
        stack.pop_back();
        for (QueryPlan::Node * child : current_node->children)
        {
            direct_parent_nodes_map[child].push_back(current_node);
            stack.push_back(child);
        }
    }
    return direct_parent_nodes_map;
}

ColumnCandidatePtr
findInputColumn(const ColumnCandidatesStack & column_candidates, QueryPlan::Node * node, const std::string & input_column_name)
{
    for (ColumnCandidatePtr column_candidate : column_candidates)
    {
        auto input_it = column_candidate->node_to_input_name.find(node);
        if (input_it != column_candidate->node_to_input_name.end() && input_it->second == input_column_name)
            return column_candidate;
    }
    return nullptr;
}

ColumnCandidatePtr
findOutputColumn(const ColumnCandidatesStack & column_candidates, QueryPlan::Node * node, const std::string & output_column_name)
{
    for (ColumnCandidatePtr column_candidate : column_candidates)
    {
        auto output_it = column_candidate->node_to_output_name.find(node);
        if (output_it != column_candidate->node_to_output_name.end() && output_it->second == output_column_name)
            return column_candidate;
    }
    return nullptr;
}


bool isColumnOnlyInputOfFunctionWithNoArrayJoin(
    const ActionsDAG::Node * function_node, const ColumnCandidatePtr & candidate, QueryPlan::Node * current_node)
{
    if (!function_node || !candidate || !current_node)
        return false;

    bool function_has_no_other_colmns_or_array_joins = true;
    bool candidate_is_used_by_function = false;

    auto input_it = candidate->node_to_input_name.find(current_node);
    if (input_it == candidate->node_to_input_name.end())
        return false;

    const std::string & candidate_input_name = input_it->second;

    for (const ActionsDAG::Node * input_node : function_node->children)
    {
        const ActionsDAG::Node * dealiased_input_node = dealiasNode(input_node);
        // we can't push down in such cases
        if ((dealiased_input_node->type == ActionsDAG::ActionType::INPUT && dealiased_input_node->result_name != candidate_input_name)
            || (dealiased_input_node->type == ActionsDAG::ActionType::ARRAY_JOIN))
            function_has_no_other_colmns_or_array_joins = false;

        if (dealiased_input_node->result_name == candidate_input_name)
            candidate_is_used_by_function = true;
    }

    return function_has_no_other_colmns_or_array_joins && candidate_is_used_by_function;
}

void analyzeFilterOrExpressionStep(QueryPlan::Node * node, ColumnCandidatesStack & candidates)
{
    ExpressionStep * expression_step = dynamic_cast<ExpressionStep *>(node->step.get());
    FilterStep * filter_step = dynamic_cast<FilterStep *>(node->step.get());

    chassert(expression_step || filter_step);

    ActionsDAG & expression_dag = expression_step ? expression_step->getExpression() : filter_step->getExpression();
    // check columns that are already passed from child nodes
    for (const SharedHeader & block : node->step->getInputHeaders())
    {
        for (const std::string & input_name : block->getNames())
        {
            ColumnCandidatePtr column_candidate = nullptr;

            for (QueryPlan::Node * child : node->children)
            {
                column_candidate = findOutputColumn(candidates, child, input_name);
                if (column_candidate)
                    break;
            }

            if (!column_candidate)
                column_candidate = candidates.emplace_back(std::make_shared<ColumnCandidate>(node, input_name, ""));
            else
                column_candidate->node_to_input_name[node] = input_name;

            if (node->step->getOutputHeader()->has(input_name))
                column_candidate->node_to_output_name[node] = input_name;
        }
    }

    // check new columns/potential functions that are not passed from child nodes
    for (const ActionsDAG::Node * output_node : expression_dag.getOutputs())
    {
        const ActionsDAG::Node * dealiased_output_node = dealiasNode(output_node);

        if (dealiased_output_node->type == ActionsDAG::ActionType::INPUT)
        {
            // these should already be added through the input header loop
            ColumnCandidatePtr column_candidate = findInputColumn(candidates, node, dealiased_output_node->result_name);

            chassert(column_candidate);

            column_candidate->node_to_output_name[node] = output_node->result_name;
        }
        else
        {
            if (dealiased_output_node->type == ActionsDAG::ActionType::FUNCTION)
                candidates.emplace_back(std::make_shared<ColumnCandidate>(node, "", output_node->result_name));
            if (dealiased_output_node->type == ActionsDAG::ActionType::FUNCTION
                && dealiased_output_node->function_base->isSuitableForPushDownBeforeFilter())
                continue;
            // update pushdown point for every column used by non suitable functions
            std::vector<const ActionsDAG::Node *> stack;
            stack.push_back(dealiased_output_node);
            while (!stack.empty())
            {
                const ActionsDAG::Node * curr = stack.back();
                stack.pop_back();
                if (curr->type == ActionsDAG::ActionType::INPUT)
                {
                    ColumnCandidatePtr column_candidate = findInputColumn(candidates, node, curr->result_name);

                    chassert(column_candidate);

                    column_candidate->max_pushdown_point = node;
                }
                for (const ActionsDAG::Node * child : curr->children)
                {
                    if (child->type != ActionsDAG::ActionType::FUNCTION || !child->function_base->isSuitableForPushDownBeforeFilter())
                        stack.push_back(child);
                }
            }
        }
    }
}

void analyzeNonExpressionStep(QueryPlan::Node * node, ColumnCandidatesStack & candidates)
{
    for (const SharedHeader & block : node->step->getInputHeaders())
    {
        for (const std::string & input_name : block->getNames())
        {
            ColumnCandidatePtr column_candidate = nullptr;
            for (QueryPlan::Node * child : node->children)
            {
                column_candidate = findOutputColumn(candidates, child, input_name);
                if (column_candidate)
                    break;
            }
            if (!column_candidate)
                continue;

            column_candidate->node_to_input_name[node] = input_name;
            column_candidate->node_to_output_name[node] = input_name;
        }
    }
    LimitStep * limit_step = typeid_cast<LimitStep *>(node->step.get());
    // for non limit steps, we may move the max push down point
    if (!limit_step)
    {
        SortingStep * sorting_step = typeid_cast<SortingStep *>(node->step.get());
        NameSet name_set;
        // if it's a sorting step, we only need to change the pushdown point for columns used in sorting
        if (sorting_step)
        {
            for (const SortColumnDescription & sort_description : sorting_step->getSortDescription())
                name_set.insert(sort_description.column_name);
        }

        for (ColumnCandidatePtr & column_candidate : candidates)
        {
            auto input_it = column_candidate->node_to_input_name.find(node);

            if (input_it == column_candidate->node_to_input_name.end())
                continue; // if the column is not an input to the node

            if (sorting_step && !name_set.contains(input_it->second))
                continue; // if the column is not used in sorting

            column_candidate->max_pushdown_point = node;
        }
    }
}

/// phase 2
PushdownRequestsStack createPushdownRequestsForColumnCandidates(QueryPlan::Node * node, const ColumnCandidatesStack & column_candidates)
{
    PushdownRequestsStack requests;

    ExpressionStep * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    FilterStep * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!expression_step && !filter_step)
        return requests;

    std::unordered_map<ColumnCandidatePtr, std::vector<ColumnCandidatePtr>> column_candidate_to_functions_map;
    std::unordered_set<ColumnCandidatePtr> unsuitable_columns;

    // find suitable functions
    for (const ColumnCandidatePtr & function_candidate : column_candidates)
    {
        auto input_it = function_candidate->node_to_input_name.find(node);
        if (input_it == function_candidate->node_to_input_name.end() || !input_it->second.empty())
            continue;

        const ActionsDAG & dag = expression_step ? expression_step->getExpression() : filter_step->getExpression();
        auto output_it = function_candidate->node_to_output_name.find(node);
        if (output_it == function_candidate->node_to_output_name.end())
            continue;

        const ActionsDAG::Node * output = dag.tryFindInOutputs(output_it->second);
        if (!output)
            continue;

        const ActionsDAG::Node * dealiased_output_node = dealiasNode(output);
        if (dealiased_output_node->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        // Check each input of the function
        for (const ActionsDAG::Node * input : dealiased_output_node->children)
        {
            const ActionsDAG::Node * dealiased_input_node = dealiasNode(input);
            if (dealiased_input_node->type != ActionsDAG::ActionType::INPUT)
                continue;

            // Find the column candidate for this input
            ColumnCandidatePtr column_candidate_for_input = nullptr;
            for (QueryPlan::Node * node_child : node->children)
            {
                column_candidate_for_input = findOutputColumn(column_candidates, node_child, dealiased_input_node->result_name);
                if (column_candidate_for_input)
                    break;
            }
            // must find as input
            if (!column_candidate_for_input)
                break;
            // this node must no longer output the column candidate
            if (column_candidate_for_input->node_to_output_name.contains(node))
            {
                unsuitable_columns.insert(column_candidate_for_input);
                break;
            }

            if (!isColumnOnlyInputOfFunctionWithNoArrayJoin(dealiased_output_node, column_candidate_for_input, node))
            {
                unsuitable_columns.insert(column_candidate_for_input);
                break;
            }

            if (dealiased_output_node->function_base && dealiased_output_node->function_base->isSuitableForPushDownBeforeFilter())
                column_candidate_to_functions_map[column_candidate_for_input].push_back(function_candidate);
            else
                unsuitable_columns.insert(column_candidate_for_input);
        }
    }

    for (const auto & [column_candidate, function_candidates] : column_candidate_to_functions_map)
    {
        if (unsuitable_columns.contains(column_candidate))
            continue;

        requests.emplace_back(std::make_shared<PushdownRequest>(node, column_candidate, function_candidates));
    }

    return requests;
}

PushdownRequestsStack aggregateParentRequests(const std::unordered_map<QueryPlan::Node *, PushdownRequestsStack> & parent_to_requests)
{
    using NodeToPushdownRequests = std::unordered_map<QueryPlan::Node *, PushdownRequestPtr>;

    std::unordered_map<ColumnCandidatePtr, NodeToPushdownRequests> column_to_requests;
    PushdownRequestsStack requests;

    for (const auto & [parent_node, parent_requests] : parent_to_requests)
    {
        for (const PushdownRequestPtr & request : parent_requests)
            column_to_requests[request->input_column_candidate][parent_node] = request;
    }

    // only return requests shared by all parents
    for (const auto & [column_candidate, request_map] : column_to_requests)
    {
        if (request_map.size() == parent_to_requests.size())
            requests.emplace_back(request_map.begin()->second);
    }
    return requests;
}

/// phase 3
const ActionsDAG::Node *
tryFindOrAddInputToExpressionFromHeader(QueryPlan::Node * node, const std::string & column_name, ActionsDAG & expression)
{
    // try to find it first
    const auto & inputs = expression.getInputs();
    for (const ActionsDAG::Node * input : inputs)
    {
        const ActionsDAG::Node * dealiased_input = dealiasNode(input);
        if (dealiased_input->type == ActionsDAG::ActionType::INPUT && input->result_name == column_name)
            return input;
    }
    // otherwise, add it from the input header
    for (const SharedHeader & block : node->step->getInputHeaders())
    {
        for (const ColumnWithTypeAndName & column : block->getColumnsWithTypeAndName())
        {
            if (column.name == column_name)
                return &(expression.addInput(column));
        }
    }
    return nullptr;
}

void updateInputsAfterChildAppliesRequest(
    QueryPlan::Node * node_to_update, QueryPlan::Node * changed_child, const PushdownRequestPtr & request)
{
    // Find index of output header that matches the current step
    size_t index = 0;
    for (; index < node_to_update->children.size(); index++)
    {
        if (node_to_update->children[index] == changed_child)
            break;
    }
    if (index == node_to_update->children.size())
        return;

    NodeToNameMap & column_input_names = request->input_column_candidate->node_to_input_name;
    NodeToNameMap & column_output_names = request->input_column_candidate->node_to_output_name;
    ExpressionStep * as_expression_step = typeid_cast<ExpressionStep *>(node_to_update->step.get());
    FilterStep * as_filter_step = typeid_cast<FilterStep *>(node_to_update->step.get());

    ActionsDAG new_dag;
    if (as_expression_step || as_filter_step)
        new_dag = as_expression_step ? as_expression_step->getExpression().clone() : as_filter_step->getExpression().clone();

    bool column_in_dag = column_input_names.find(node_to_update) != column_input_names.end();

    for (const ColumnCandidatePtr & function_candidate : request->function_column_candidates)
    {
        // before proceeding, the input column must be already in the DAG OR the function node must be already in the DAG
        // if not, skip the function candidate
        const ActionsDAG::Node * new_function_node = nullptr;
        NodeToNameMap & function_output_names = function_candidate->node_to_output_name;
        NodeToNameMap & function_input_names = function_candidate->node_to_input_name;

        bool function_in_dag = function_input_names.find(node_to_update) != function_input_names.end();

        if (!column_in_dag && !function_in_dag)
            continue;

        if (as_expression_step || as_filter_step)
        {
            const ActionsDAG::Node * function_node_in_original_node = request->getFunctionNode(function_candidate);
            const ActionsDAG::Node * input_node = nullptr;

            for (const ActionsDAG::Node * node : new_dag.getInputs())
            {
                if (node->result_name == function_output_names[changed_child])
                    input_node = node;
            }
            if (!input_node)
            {
                chassert(function_node_in_original_node);
                input_node = &new_dag.addInput(function_output_names[changed_child], function_node_in_original_node->result_type);
            }

            // if function is aliased at the parent, ensure we use the same alias
            if (function_output_names.find(node_to_update) != function_output_names.end())
                new_function_node = &new_dag.addAlias(*input_node, function_output_names[node_to_update]);
            else
                new_function_node = input_node;

            // either replace input column or function instance with the new function node
            if (column_output_names.find(node_to_update) != column_output_names.end())
            {
                if (new_dag.tryFindInOutputs(column_output_names[node_to_update]))
                    new_dag.removeUnusedResult(column_output_names[node_to_update]);
                column_output_names.erase(node_to_update);
            }

            new_dag.addOrReplaceInOutputs(*new_function_node);
            new_dag.removeUnusedActions(false);
        }

        function_input_names[node_to_update] = function_output_names[changed_child];
        function_output_names[node_to_update] = new_function_node ? new_function_node->result_name : function_input_names[node_to_update];
    }

    if (as_expression_step || as_filter_step)
    {
        if (as_expression_step)
        {
            node_to_update->step = std::make_unique<ExpressionStep>(changed_child->step->getOutputHeader(), std::move(new_dag));
        }
        else
        {
            node_to_update->step = std::make_unique<FilterStep>(
                changed_child->step->getOutputHeader(),
                std::move(new_dag),
                as_filter_step->getFilterColumnName(),
                as_filter_step->removesFilterColumn());
        }
    }
    else
    {
        node_to_update->step->updateInputHeader(changed_child->step->getOutputHeader(), index);
    }
}

void propagateExecutedRequestToParents(
    QueryPlan::Node * starting_node, const PushdownRequestPtr & request, NodeToParentsMap & direct_parent_nodes_map)
{
    std::unordered_set<QueryPlan::Node *> visited_nodes;
    std::vector<QueryPlan::Node *> children_stack;
    children_stack.emplace_back(starting_node);
    visited_nodes.insert(starting_node);
    while (!children_stack.empty())
    {
        QueryPlan::Node * child_node = children_stack.back();
        children_stack.pop_back();
        std::vector<QueryPlan::Node *> parents_stack(direct_parent_nodes_map[child_node]);
        while (!parents_stack.empty())
        {
            QueryPlan::Node * node_to_update = parents_stack.back();
            parents_stack.pop_back();
            if (visited_nodes.contains(node_to_update))
                continue;
            visited_nodes.insert(node_to_update);

            updateInputsAfterChildAppliesRequest(node_to_update, child_node, request);

            children_stack.push_back(node_to_update);
        }
    }
}


bool tryExecuteRequestOnStepOrParents(
    QueryPlan::Node * node, const PushdownRequestPtr & request, NodeToParentsMap & direct_parent_nodes_map)
{
    ExpressionStep * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    FilterStep * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!expression_step && !filter_step)
    {
        // if the node is not an expression or filter step, try to execute the request on its parents
        // this can happen if the node is a limit step, for example
        for (QueryPlan::Node * parent_node : direct_parent_nodes_map[node])
        {
            if (!parent_node)
                continue;
            tryExecuteRequestOnStepOrParents(parent_node, request, direct_parent_nodes_map);
        }
        return false;
    }

    ActionsDAG expression = expression_step ? expression_step->getExpression().clone() : filter_step->getExpression().clone();
    const ActionsDAG::Node * node_to_remove = nullptr;
    for (ColumnCandidatePtr & function_candidate : request->function_column_candidates)
    {
        const ActionsDAG::Node * function_node = request->getFunctionNode(function_candidate);
        std::vector<const ActionsDAG::Node *> new_children;
        for (const ActionsDAG::Node * function_child_node : function_node->children)
        {
            const ActionsDAG::Node * dealiased_child_node = dealiasNode(function_child_node);
            const ActionsDAG::Node * child_node_in_expression = expression.tryFindInOutputs(dealiased_child_node->result_name);

            // input can be in the header but the not the dag inputs
            if (!child_node_in_expression)
                child_node_in_expression = tryFindOrAddInputToExpressionFromHeader(node, dealiased_child_node->result_name, expression);
            else if (dealiased_child_node->type == ActionsDAG::ActionType::INPUT)
                node_to_remove = child_node_in_expression;

            switch (dealiased_child_node->type)
            {
                case ActionsDAG::ActionType::COLUMN:
                    if (!child_node_in_expression)
                    {
                        ColumnWithTypeAndName column;
                        column.column = dealiased_child_node->column;
                        column.name = dealiased_child_node->result_name;
                        column.type = dealiased_child_node->result_type;
                        new_children.push_back(&expression.addColumn(column));
                    }
                    else
                    {
                        new_children.push_back(child_node_in_expression);
                    }
                    break;
                default:
                    if (!child_node_in_expression)
                        return false;
                    new_children.push_back(child_node_in_expression);
                    break;
            }
        }

        const ActionsDAG::Node * new_function_node = nullptr;

        // if the function node is already there and is aliased, make sure to use the same alias
        NodeToNameMap & function_output_names = function_candidate->node_to_output_name;
        if (function_output_names.find(node) != function_output_names.end())
            new_function_node = &expression.addAlias(
                expression.addFunction(function_node->function_base, new_children, function_node->result_name),
                function_output_names[node]);
        else
            new_function_node = &expression.addFunction(function_node->function_base, new_children, function_node->result_name);


        expression.addOrReplaceInOutputs(*new_function_node);

        function_output_names[node] = new_function_node->result_name;
    }

    if (node_to_remove)
    {
        if (expression.tryFindInOutputs(node_to_remove->result_name))
            expression.removeUnusedResult(node_to_remove->result_name);
        request->input_column_candidate->node_to_output_name.erase(node);
    }

    if (expression_step)
        node->step = std::make_unique<ExpressionStep>(node->children[0]->step->getOutputHeader(), std::move(expression));
    else
        node->step = std::make_unique<FilterStep>(
            node->children[0]->step->getOutputHeader(),
            std::move(expression),
            filter_step->getFilterColumnName(),
            filter_step->removesFilterColumn());
    propagateExecutedRequestToParents(node, request, direct_parent_nodes_map);
    return true;
}

/// entry function
void
processNodeDFS(QueryPlan::Node * node, ColumnCandidatesStack column_candidates, NodeToParentsMap & direct_parent_nodes_map)
{
    std::unordered_map<QueryPlan::Node *, PushdownRequestsStack> all_node_requests;
    std::vector<QueryPlan::Node *> stack;
    std::unordered_set<QueryPlan::Node *> visited;
    std::vector<QueryPlan::Node *> processing_order;

    stack.push_back(node);

    while (!stack.empty())
    {
        QueryPlan::Node * current_node = stack.back();

        if (visited.contains(current_node))
        {
            stack.pop_back();
            continue;
        }

        // Check if all parents have been processed
        bool all_parents_processed = true;
        const std::vector<QueryPlan::Node *> & parent_nodes = direct_parent_nodes_map[current_node];
        for (QueryPlan::Node * parent_node : parent_nodes)
        {
            if (!visited.contains(parent_node))
            {
                all_parents_processed = false;
                stack.push_back(parent_node);
            }
        }

        if (!all_parents_processed)
            continue;

        // Process current node
        visited.insert(current_node);
        processing_order.push_back(current_node);
        stack.pop_back();

        // Phase 1: Analyze current node to update column candidates
        FilterStep * filter_step = typeid_cast<FilterStep *>(current_node->step.get());
        ExpressionStep * expression_step = typeid_cast<ExpressionStep *>(current_node->step.get());

        if (filter_step || expression_step)
            analyzeFilterOrExpressionStep(current_node, column_candidates);
        else
            analyzeNonExpressionStep(current_node, column_candidates);

        // Phase 2: Create pushdown requests for current node
        PushdownRequestsStack local_requests = createPushdownRequestsForColumnCandidates(current_node, column_candidates);

        // Aggregate parent requests
        std::unordered_map<QueryPlan::Node *, PushdownRequestsStack> parent_to_requests;
        for (QueryPlan::Node * parent_node : parent_nodes)
            parent_to_requests[parent_node] = all_node_requests[parent_node];

        PushdownRequestsStack parents_requests = aggregateParentRequests(parent_to_requests);

        // Combine local and parent requests
        PushdownRequestsStack combined_requests;
        combined_requests.insert(combined_requests.end(), local_requests.begin(), local_requests.end());
        combined_requests.insert(combined_requests.end(), parents_requests.begin(), parents_requests.end());

        all_node_requests[current_node] = combined_requests;
    }

    // Phase 3: Execute pushdown requests in reverse processing order
    for (auto it = processing_order.rbegin(); it != processing_order.rend(); ++it)
    {
        QueryPlan::Node * current_node = *it;
        const PushdownRequestsStack & requests = all_node_requests[current_node];

        for (const PushdownRequestPtr & request : requests)
        {
            const ColumnCandidatePtr & column_candidate = request->input_column_candidate;
            if (column_candidate && column_candidate->max_pushdown_point == current_node)
                tryExecuteRequestOnStepOrParents(current_node, request, direct_parent_nodes_map);
        }
    }
}

QueryPlan::Node * findSourceNode(QueryPlan::Node * root_node)
{
    std::vector<QueryPlan::Node *> nodes_stack;
    nodes_stack.push_back(root_node);
    while (!nodes_stack.empty())
    {
        QueryPlan::Node * current_node = nodes_stack.back();
        nodes_stack.pop_back();
        if (dynamic_cast<ISourceStep *>(current_node->step.get()))
        {
            return current_node;
        }
        for (QueryPlan::Node * child_node : current_node->children)
        {
            nodes_stack.push_back(child_node);
        }
    }
    return nullptr;
}
}

void pushDownVolumeReducingFunctions(QueryPlan::Node * root_node, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.push_down_volume_reducing_functions)
        return;

    QueryPlan::Node * node = findSourceNode(root_node);

    if (!node)
        return;

    NodeToParentsMap direct_parent_nodes_map = buildDirectParentNodesMap(root_node);
    processNodeDFS(node, {}, direct_parent_nodes_map);
}

}
}
