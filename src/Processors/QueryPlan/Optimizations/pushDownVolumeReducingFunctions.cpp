#include <memory>
#include <unordered_map>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include "Common/Exception.h"
#include "Core/Names.h"
#include "Core/SortDescription.h"
#include "Processors/QueryPlan/LimitStep.h"
#include "Processors/QueryPlan/QueryPlan.h"
#include "Processors/QueryPlan/SortingStep.h"
namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{
namespace
{

using DirectParentNodesMap = std::unordered_map<QueryPlan::Node *, std::vector<QueryPlan::Node *>>;
using NodeToNameMap = std::unordered_map<QueryPlan::Node *, std::string>;

/// Represents a column that can potentially have functions pushed down through it
struct ColumnCandidate
{
    QueryPlan::Node * max_pushdown_point;
    int max_pushdown_depth;
    NodeToNameMap node_to_input_name;
    NodeToNameMap node_to_output_name;

    [[maybe_unused]] ColumnCandidate(
        QueryPlan::Node * initial_node, int initial_depth, const std::string & input_name, const std::string & output_name)
        : max_pushdown_point(initial_node)
        , max_pushdown_depth(initial_depth)
        , node_to_input_name({{initial_node, input_name}})
        , node_to_output_name({{initial_node, output_name}})
    {
    }
};
using ColumnCandidatePtr = std::shared_ptr<ColumnCandidate>;
using ColumnCandidatesStack = std::vector<ColumnCandidatePtr>;

/// Represents a request to push down a volume-reducing function
struct PushdownRequest
{
    const ActionsDAG::Node * function_node;
    std::shared_ptr<ColumnCandidate> input_column_candidate;
    std::shared_ptr<ColumnCandidate> function_column_candidate;

    [[maybe_unused]] PushdownRequest(
        const ActionsDAG::Node * node, const ColumnCandidatePtr & input_candidate, const ColumnCandidatePtr & function_candidate)
        : function_node(node)
        , input_column_candidate(input_candidate)
        , function_column_candidate(function_candidate)
    {
    }
};

using PushdownRequestPtr = std::shared_ptr<PushdownRequest>;
using PushdownRequestsStack = std::vector<PushdownRequestPtr>;

inline const ActionsDAG::Node * dealiasNode(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();
    return node;
}

/// phase 1

DirectParentNodesMap buildDirectParentNodesMap(QueryPlan::Node * root_node)
{
    DirectParentNodesMap direct_parent_nodes_map;
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
    for (const auto & column_candidate : column_candidates)
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
    for (const auto & column_candidate : column_candidates)
    {
        auto output_it = column_candidate->node_to_output_name.find(node);
        if (output_it != column_candidate->node_to_output_name.end() && output_it->second == output_column_name)
            return column_candidate;
    }
    return nullptr;
}


bool isColumnCandidateOnlyInputOfFunction(
    const ActionsDAG::Node * function_node, const ColumnCandidatePtr & candidate, QueryPlan::Node * current_node)
{
    if (!function_node || !candidate || !current_node)
        return false;

    bool the_candidate_is_the_only_input_of_the_function = true;
    bool the_candidate_is_there = false;

    auto input_it = candidate->node_to_input_name.find(current_node);
    if (input_it == candidate->node_to_input_name.end())
        return false;

    const std::string & candidate_input_name = input_it->second;

    for (const ActionsDAG::Node * input_node : function_node->children)
    {
        const ActionsDAG::Node * dealiased_input_node = dealiasNode(input_node);

        if (dealiased_input_node->type == ActionsDAG::ActionType::INPUT && dealiased_input_node->result_name != candidate_input_name)
            the_candidate_is_the_only_input_of_the_function = false;

        if (dealiased_input_node->result_name == candidate_input_name)
            the_candidate_is_there = true;
    }

    return the_candidate_is_the_only_input_of_the_function && the_candidate_is_there;
}

void analyzeFilterStep(QueryPlan::Node * node, FilterStep * filter_step, int depth, ColumnCandidatesStack & candidates)
{
    const auto & outputs = filter_step->getExpression().getOutputs();

    for (const ActionsDAG::Node * output : outputs)
    {
        std::string output_name = output->result_name;
        std::string input_name = output_name;

        if (output->type == ActionsDAG::ActionType::ALIAS)
        {
            std::string alias_target = dealiasNode(output)->result_name;
            if (!alias_target.empty())
                input_name = alias_target;
        }

        // Try to find existing column candidate from child nodes based on the input name
        ColumnCandidatePtr column_candidate = nullptr;
        for (QueryPlan::Node * child : node->children)
        {
            column_candidate = findOutputColumn(candidates, child, input_name);
            if (column_candidate)
                break;
        }

        if (!column_candidate)
        {
            candidates.emplace_back(std::make_shared<ColumnCandidate>(node, depth, input_name, output_name));
        }
        else
        {
            column_candidate->node_to_input_name[node] = input_name;
            column_candidate->node_to_output_name[node] = output_name;
        }
    }
}

void analyzeExpressionStep(QueryPlan::Node * node, ExpressionStep * expression_step, ColumnCandidatesStack & candidates, int depth)
{
    for (const auto & block : node->step->getInputHeaders())
    {
        for (const auto & input_name : block->getNames())
        {
            ColumnCandidatePtr column_candidate = nullptr;

            for (QueryPlan::Node * child : node->children)
            {
                column_candidate = findOutputColumn(candidates, child, input_name);
                if (column_candidate)
                    break;
            }
            if (!column_candidate)
                column_candidate = findInputColumn(candidates, node, input_name);
            if (!column_candidate)
            {
                column_candidate = candidates.emplace_back(std::make_shared<ColumnCandidate>(node, depth, input_name, ""));
                if (node->step->getOutputHeader()->has(input_name))
                    column_candidate->node_to_output_name[node] = input_name;
            }
            else
            {
                column_candidate->node_to_input_name[node] = input_name;
                if (node->step->getOutputHeader()->has(input_name))
                    column_candidate->node_to_output_name[node] = input_name;
            }
        }
    }

    for (const ActionsDAG::Node * output_node : expression_step->getExpression().getOutputs())
    {
        const ActionsDAG::Node * dealiased_output_node = dealiasNode(output_node);

        if (dealiased_output_node->type == ActionsDAG::ActionType::INPUT)
        {
            ColumnCandidatePtr column_candidate = findInputColumn(candidates, node, dealiased_output_node->result_name);
            if (!column_candidate)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column candidate not found for output {}", dealiased_output_node->result_name);
            column_candidate->node_to_output_name[node] = output_node->result_name; // use alias
        }
        else
        {
            candidates.emplace_back(std::make_shared<ColumnCandidate>(node, depth, "", output_node->result_name));
        }
    }
}

void analyzeNonExpressionStep(
    QueryPlan::Node * node, ColumnCandidatesStack & candidates, int depth, LimitStep * limit_step, SortingStep * sorting_step)
{
    for (const auto & block : node->step->getInputHeaders())
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

    if (!limit_step)
    {
        NameSet name_set;
        if (sorting_step)
        {
            for (const SortColumnDescription & sort_description : sorting_step->getSortDescription())
                name_set.insert(sort_description.column_name);
        }

        for (auto & column_candidate : candidates)
        {
            auto input_it = column_candidate->node_to_input_name.find(node);
            if (sorting_step
                && ((input_it != column_candidate->node_to_input_name.end() && !name_set.contains(input_it->second))
                    || input_it == column_candidate->node_to_input_name.end()))
            {
                continue;
            }
            if (input_it == column_candidate->node_to_input_name.end())
                continue;

            column_candidate->max_pushdown_point = node;
            column_candidate->max_pushdown_depth = depth;
        }
    }
}

/// phase 2
PushdownRequestsStack createPushdownRequestsForColumnCandidates(QueryPlan::Node * node, const ColumnCandidatesStack & column_candidates)
{
    PushdownRequestsStack requests;

    for (const auto & column_candidate : column_candidates)
    {
        auto input_it = column_candidate->node_to_input_name.find(node);
        if (input_it == column_candidate->node_to_input_name.end() || !input_it->second.empty())
            continue;

        auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
        if (!expression_step)
            continue;

        const auto & dag = expression_step->getExpression();
        auto output_it = column_candidate->node_to_output_name.find(node);
        if (output_it == column_candidate->node_to_output_name.end())
            continue;

        const ActionsDAG::Node * output = dag.tryFindInOutputs(output_it->second);
        if (!output)
            continue;

        const ActionsDAG::Node * dealiased_output_node = dealiasNode(output);
        if (dealiased_output_node->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        // Check each input of the function
        for (const auto & input : dealiased_output_node->children)
        {
            const ActionsDAG::Node * dealiased_input_node = dealiasNode(input);
            if (dealiased_input_node->type != ActionsDAG::ActionType::INPUT)
                continue;

            // Find the column candidate for this input
            ColumnCandidatePtr column_candidate_for_input = nullptr;
            for (const auto & node_child : node->children)
            {
                column_candidate_for_input = findOutputColumn(column_candidates, node_child, dealiased_input_node->result_name);
                if (column_candidate_for_input)
                    break;
            }

            const bool no_longer_outputs_original_column
                = !(column_candidate_for_input && column_candidate_for_input->node_to_output_name.contains(node));

            if (column_candidate_for_input && no_longer_outputs_original_column
                && isColumnCandidateOnlyInputOfFunction(dealiased_output_node, column_candidate_for_input, node)
                && dealiased_output_node->function_base && dealiased_output_node->function_base->isSuitableForPushDownBeforeFilter())
            {
                requests.emplace_back(
                    std::make_shared<PushdownRequest>(dealiased_output_node, column_candidate_for_input, column_candidate));
            }
        }
    }

    return requests;
}

using PushdownRequestMap = std::unordered_map<QueryPlan::Node *, PushdownRequestPtr>;
using ColumnCandidatesToRequestsMap = std::unordered_map<ColumnCandidatePtr, PushdownRequestMap>;

PushdownRequestsStack aggregateParentRequests(const std::unordered_map<QueryPlan::Node *, PushdownRequestsStack> & parent_to_requests)
{
    ColumnCandidatesToRequestsMap column_to_requests;
    PushdownRequestsStack requests;

    for (const auto & [parent_node, parent_requests] : parent_to_requests)
    {
        for (const auto & request : parent_requests)
            column_to_requests[request->input_column_candidate][parent_node] = request;
    }

    for (const auto & [column_candidate, request_map] : column_to_requests)
    {
        // remove requests for columns that are still needed by other parents
        if (request_map.size() == parent_to_requests.size())
            requests.emplace_back(request_map.begin()->second);
    }
    return requests;
}

/// phase 3

void executeRequestOnStep(
    QueryPlan::Node * node, const PushdownRequestPtr & request, FilterStep * filter_step, ExpressionStep * expression_step)
{
    if (!filter_step && !expression_step)
        return;

    ActionsDAG expression = expression_step ? expression_step->getExpression().clone() : filter_step->getExpression().clone();

    const ActionsDAG::Node * node_to_remove = nullptr;
    std::vector<const ActionsDAG::Node *> new_children;
    for (const auto & input_node : request->function_node->children)
    {
        const ActionsDAG::Node * dealiased_input_node = dealiasNode(input_node);

        if (dealiased_input_node->type == ActionsDAG::ActionType::INPUT)
        {
            chassert(node_to_remove == nullptr); // expecting single input node
            const ActionsDAG::Node * input_node_in_expression = expression.tryFindInOutputs(dealiased_input_node->result_name);
            if (!input_node_in_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Input node not found in expression");
            new_children.push_back(input_node_in_expression);
            node_to_remove = dealiased_input_node;
        }
        else
        {
            new_children.push_back(input_node);
        }
    }

    chassert(request->function_node->function_base && node_to_remove);

    const ActionsDAG::Node * new_function_node
        = &expression.addFunction(request->function_node->function_base, new_children, request->function_node->result_name);

    NodeToNameMap & function_output_names = request->function_column_candidate->node_to_output_name;

    if (function_output_names.contains(node) && !function_output_names[node].empty()
        && function_output_names[node] != new_function_node->result_name)
    {
        expression.removeUnusedResult(function_output_names[node]);
        expression.getOutputs().push_back(&expression.addAlias(*new_function_node, function_output_names[node]));
    }
    else
    {
        expression.getOutputs().push_back(new_function_node);
        function_output_names[node] = new_function_node->result_name;
    }

    expression.removeUnusedResult(node_to_remove->result_name);

    if (expression_step)
    {
        node->step = std::make_unique<ExpressionStep>(node->children[0]->step->getOutputHeader(), std::move(expression));
    }
    else
    {
        node->step = std::make_unique<FilterStep>(
            node->children[0]->step->getOutputHeader(),
            std::move(expression),
            filter_step->getFilterColumnName(),
            filter_step->removesFilterColumn());
    }
}

void updateParentSteps(QueryPlan::Node * node, const PushdownRequestPtr & request, DirectParentNodesMap & direct_parent_nodes_map)
{
    const auto & parent_nodes = direct_parent_nodes_map[node];

    std::vector<QueryPlan::Node *> nodes_stack(parent_nodes.rbegin(), parent_nodes.rend());
    QueryPlan::Node * current_bottom_node = node;

    while (!nodes_stack.empty())
    {
        QueryPlan::Node * node_to_update = nodes_stack.back();
        nodes_stack.pop_back();

        // Find index of output header that matches the current step
        size_t index = 0;
        for (; index < node_to_update->children.size(); index++)
        {
            if (node_to_update->children[index] == current_bottom_node)
                break;
        }
        if (index == node_to_update->children.size())
            break;

        NodeToNameMap & function_output_names = request->function_column_candidate->node_to_output_name;
        NodeToNameMap & function_input_names = request->function_column_candidate->node_to_input_name;

        if (auto * node_to_update_as_expression_step = typeid_cast<ExpressionStep *>(node_to_update->step.get()))
        {
            ActionsDAG new_dag(current_bottom_node->step->getOutputHeader()->getNamesAndTypesList());
            ActionsDAG existing_dag = node_to_update_as_expression_step->getExpression().clone();


            const ActionsDAG::Node * function_node_in_new_dag = new_dag.tryFindInOutputs(function_output_names[current_bottom_node]);

            const ActionsDAG::Node * function_node_in_existing_dag = existing_dag.tryFindInOutputs(function_output_names[node_to_update]);

            // Get the column name for this request from the mappings
            auto name_it = request->input_column_candidate->node_to_output_name.find(node_to_update);
            if (name_it != request->input_column_candidate->node_to_output_name.end())
            {
                const ActionsDAG::Node * column_node_in_existing_dag = existing_dag.tryFindInOutputs(name_it->second);
                if (column_node_in_existing_dag)
                    existing_dag.removeUnusedResult(column_node_in_existing_dag->result_name);
            }

            if (function_node_in_existing_dag && function_node_in_new_dag)
            {
                if (function_node_in_existing_dag)
                    existing_dag.removeUnusedResult(function_output_names[node_to_update]);

                new_dag.getOutputs().push_back(&new_dag.addAlias(*function_node_in_new_dag, function_output_names[node_to_update]));
                new_dag.removeUnusedResult(function_node_in_new_dag->result_name);
            }

            new_dag.mergeInplace(std::move(existing_dag));
            node_to_update->step
                = std::make_unique<ExpressionStep>(node_to_update->children[0]->step->getOutputHeader(), std::move(new_dag));

            function_input_names[node_to_update] = function_output_names[current_bottom_node];
            function_output_names[node_to_update]
                = function_node_in_existing_dag ? function_node_in_existing_dag->result_name : function_input_names[node_to_update];
        }
        else
        {
            node_to_update->step->updateInputHeader(current_bottom_node->step->getOutputHeader(), index);
            function_input_names[node_to_update] = function_output_names[current_bottom_node];
            function_output_names[node_to_update] = function_input_names[node_to_update];
        }

        current_bottom_node = node_to_update;
        const auto & parent_nodes_to_update = direct_parent_nodes_map[node_to_update];
        nodes_stack.insert(nodes_stack.begin(), parent_nodes_to_update.rbegin(), parent_nodes_to_update.rend());
    }
}

/// main function
PushdownRequestsStack processNodeRecursively(
    QueryPlan::Node * node, ColumnCandidatesStack column_candidates, int depth, DirectParentNodesMap & direct_parent_nodes_map)
{
    PushdownRequestsStack requests;

    // Phase 1: Analyze current node to update column candidates
    FilterStep * filter_step = typeid_cast<FilterStep *>(node->step.get());
    ExpressionStep * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    LimitStep * limit_step = typeid_cast<LimitStep *>(node->step.get());
    SortingStep * sorting_step = typeid_cast<SortingStep *>(node->step.get());

    if (filter_step)
        analyzeFilterStep(node, filter_step, depth, column_candidates);
    else if (expression_step)
        analyzeExpressionStep(node, expression_step, column_candidates, depth);
    else
        analyzeNonExpressionStep(node, column_candidates, depth, limit_step, sorting_step);

    // Phase 2: Create pushdown requests for current node
    PushdownRequestsStack local_requests = createPushdownRequestsForColumnCandidates(node, column_candidates);
    requests.insert(requests.end(), local_requests.begin(), local_requests.end());

    // Process parents before execution
    const auto & parent_nodes = direct_parent_nodes_map[node];
    std::unordered_map<QueryPlan::Node *, PushdownRequestsStack> parent_to_requests;
    for (QueryPlan::Node * parent_node : parent_nodes)
        parent_to_requests[parent_node] = processNodeRecursively(parent_node, column_candidates, depth + 1, direct_parent_nodes_map);

    // Aggregate parent requests
    PushdownRequestsStack parents_requests = aggregateParentRequests(parent_to_requests);
    requests.insert(requests.end(), parents_requests.begin(), parents_requests.end());

    // Phase 3: Execute pushdown requests
    for (const PushdownRequestPtr & request : parents_requests)
    {
        const ColumnCandidatePtr & column_candidate = request->input_column_candidate;
        if ((expression_step || filter_step) && column_candidate && column_candidate->max_pushdown_point == node)
        {
            // These steps can get recreated; read latest
            if (filter_step)
                filter_step = typeid_cast<FilterStep *>(node->step.get());
            if (expression_step)
                expression_step = typeid_cast<ExpressionStep *>(node->step.get());

            executeRequestOnStep(node, request, filter_step, expression_step);
            updateParentSteps(node, request, direct_parent_nodes_map);
        }
    }

    return requests;
}

}

void pushDownVolumeReducingFunctions(
    QueryPlan::Node * node, QueryPlan::Node * root_node, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.push_down_volume_reducing_functions)
        return;

    if (!node || node->children.empty())
        return;

    FilterStep * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!filter_step)
        return;

    DirectParentNodesMap direct_parent_nodes_map = buildDirectParentNodesMap(root_node);
    processNodeRecursively(node, {}, 0, direct_parent_nodes_map);
}

} // namespace QueryPlanOptimizations
} // namespace DB
