#include <Interpreters/ActionsDAG.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <base/types.h>
#include <vector>


namespace DB::QueryPlanOptimizations
{

namespace
{

/// Represents a list of conjunctive or disjunctive conditions
/// Some sub-conditions are references to the existing nodes in the DAG, others are conjctions or disjunctions
/// that are not present in the original DAG
/// Example: when we have a filter on 2 tables n1 and n2:
///     (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR
///     (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
/// the goal is to extract separate partial filters for each table
/// for n1:
///     n1.n_name = 'FRANCE' OR n1.n_name = 'GERMANY'
/// for n2:
///     n2.n_name = 'GERMANY' OR n2.n_name = 'FRANCE'
///
/// The idea is to traverse the all conjunctions and disjunctions in the original condition and
///  1. replace elements of conjunctions that depend on other columns with 'True'
///  2. replace the whole disjunctions with 'True' if any element depends on other columns
struct ConditionList
{
    enum ConditionType
    {
        SingleCondition,
        And,
        Or
    };

    ConditionType condition_type = SingleCondition;
    std::vector<const ActionsDAG::Node *> existing_nodes;
    std::vector<ConditionList> subconditions;

    bool assumedTrue() const
    {
        return existing_nodes.empty() &&
            (subconditions.empty() || (subconditions.size() == 1 && subconditions.front().assumedTrue()));
    }
};

/// Check if the whole subgraph that calculates the node only uses columns from the list
bool onlyDependsOnAvailableColumns(const ActionsDAG::Node & node, const NameSet & available_columns)
{
    if (node.type == ActionsDAG::ActionType::INPUT)
    {
        return available_columns.contains(node.result_name);
    }
    else
    {
        for (const auto * child : node.children)
        {
            if (!onlyDependsOnAvailableColumns(*child, available_columns))
                return false;
        }
        return true;
    }
}

/// Extract all conditions from the filter that use only columns from the list.
/// The resulting condition is broader then the full filter but can be used to early pre-filtering of the data
ConditionList extractPartialPredicate(const ActionsDAG::Node & node, const NameSet & available_columns)
{
    const bool is_conjunction = node.type == ActionsDAG::ActionType::FUNCTION && node.function_base->getName() == "and";
    const bool is_disjunction = node.type == ActionsDAG::ActionType::FUNCTION && node.function_base->getName() == "or";
    if (is_conjunction)
    {
        ConditionList conjuction_template;
        conjuction_template.condition_type = ConditionList::And;
        for (const auto * child : node.children)
        {
            auto child_condition_template = extractPartialPredicate(*child, available_columns);
            if (!child_condition_template.assumedTrue())
                conjuction_template.subconditions.emplace_back(std::move(child_condition_template));
        }
        return conjuction_template;
    }
    else if (is_disjunction)
    {
        ConditionList disjuction_template;
        disjuction_template.condition_type = ConditionList::Or;
        for (const auto * child : node.children)
        {
            auto child_condition_template = extractPartialPredicate(*child, available_columns);
            if (child_condition_template.assumedTrue())
                return ConditionList{}; /// The whole disjunction can be assumed True, so it in not included into partial predicate

            disjuction_template.subconditions.emplace_back(std::move(child_condition_template));
        }
        return disjuction_template;
    }
    else if (node.type == ActionsDAG::ActionType::ALIAS)
    {
        return extractPartialPredicate(*node.children.front(), available_columns);
    }
    else
    {
        if (!onlyDependsOnAvailableColumns(node, available_columns))
            return ConditionList{}; /// If the subtree depends on other columns then we cannot include it into partial predicate, so assume it's True

        return ConditionList{
            .condition_type = ConditionList::SingleCondition,
            .existing_nodes = {&node},
            .subconditions = {}
        };
    }
}

const ActionsDAG::Node * buildPredicateFromTemplate(ActionsDAG & full_dag, const ConditionList & predicate_template)
{
    if (predicate_template.assumedTrue())
        return nullptr;

    /// Just 1 element?
    if (predicate_template.existing_nodes.size() + predicate_template.subconditions.size() == 1)
    {
        if (predicate_template.existing_nodes.size() == 1)
            return predicate_template.existing_nodes.front();
        else
            return buildPredicateFromTemplate(full_dag, predicate_template.subconditions.front());
    }

    ActionsDAG::NodeRawConstPtrs children(predicate_template.existing_nodes);

    for (const auto & subcondition : predicate_template.subconditions)
    {
        const auto * subcondition_node = buildPredicateFromTemplate(full_dag, subcondition);
        if (subcondition_node)
            children.push_back(subcondition_node);
    }

    if (children.size() == 1)
        return children.front();

    chassert(predicate_template.condition_type == ConditionList::And || predicate_template.condition_type == ConditionList::Or);

    FunctionOverloadResolverPtr function =
        predicate_template.condition_type == ConditionList::And ?
        std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>()) :
        std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());

    return &full_dag.addFunction(function, children, {});
}

}

std::optional<ActionsDAG> tryToExtractPartialPredicate(
    const ActionsDAG & original_dag,
    const std::string & filter_name,
    const Names & available_columns)
{
    if (!original_dag.tryFindInOutputs(filter_name))
        return {};

    ActionsDAG full_dag = original_dag.clone();

    const ActionsDAG::Node & predicate = full_dag.findInOutputs(filter_name);

    auto predicate_template = extractPartialPredicate(predicate, NameSet(available_columns.begin(), available_columns.end()));

    if (predicate_template.assumedTrue())
        return {};

    const auto * predicate_node = buildPredicateFromTemplate(full_dag, predicate_template);

    if (!predicate_node)
        return {};

    full_dag.getOutputs().clear();
    full_dag.addOrReplaceInOutputs(*predicate_node);
    full_dag.removeUnusedActions();

    return full_dag;
}

void addFilterOnTop(QueryPlan::Node & join_node, size_t child_idx, QueryPlan::Nodes & nodes, ActionsDAG filter_dag)
{
    auto & new_filter_node = nodes.emplace_back();
    new_filter_node.children = {join_node.children[child_idx]};
    join_node.children[child_idx] = &new_filter_node;

    auto filter_column_name = filter_dag.getOutputs().front()->result_name;
    for (const auto * input : filter_dag.getInputs())
        filter_dag.addOrReplaceInOutputs(*input);

    const auto input_header = new_filter_node.children.at(0)->step->getOutputHeader();

    new_filter_node.step = std::make_unique<FilterStep>(
        input_header,
        std::move(filter_dag),
        filter_column_name,
        true);
}

}
