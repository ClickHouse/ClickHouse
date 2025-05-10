#include <Planner/PlannerCorrelatedSubqueries.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinInfo.h>

#include <Parsers/SelectUnionMode.h>

#include <Planner/Planner.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerJoinsLogical.h>
#include <Planner/Utils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <memory>
#include <string_view>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;

}

namespace Setting
{

extern const SettingsBool join_use_nulls;

}

void CorrelatedSubtrees::assertEmpty(std::string_view reason) const
{
    if (notEmpty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Correlated subqueries {} are not supported", reason);
}

namespace
{

using CorrelatedPlanStepMap = std::unordered_map<QueryPlan::Node *, bool>;

CorrelatedPlanStepMap buildCorrelatedPlanStepMap(QueryPlan & correlated_query_plan)
{
    CorrelatedPlanStepMap result;

    struct State
    {
        QueryPlan::Node * node;
        bool processed_children = false;
    };

    std::vector<State> nodes_to_process{ { .node = correlated_query_plan.getRootNode() } };
    while (!nodes_to_process.empty())
    {
        size_t current_index = nodes_to_process.size() - 1;
        if (nodes_to_process[current_index].processed_children)
        {
            auto * current = nodes_to_process[current_index].node;

            auto & value = result[current];
            value = current->step->hasCorrelatedExpressions();

            for (auto * child : current->children)
                value |= result[child];

            nodes_to_process.pop_back();
        }
        else
        {
            for (auto * child : nodes_to_process[current_index].node->children)
                nodes_to_process.push_back({ .node = child });
            nodes_to_process[current_index].processed_children = true;
        }
    }

    return result;
}

struct DecorrelationContext
{
    const CorrelatedSubquery & correlated_subquery;
    const PlannerContextPtr & planner_context;
    QueryPlan query_plan; // LHS plan
    QueryPlan correlated_query_plan;
    CorrelatedPlanStepMap correlated_plan_steps;
};

/// Correlated subquery is represented by implicit dependent join operator.
/// This function builds a query plan to evaluate correlated subquery by
/// pushing dependent join down and replacing it with CROSS JOIN.
QueryPlan decorrelateQueryPlan(
    DecorrelationContext & context,
    QueryPlan::Node * node
)
{
    if (!context.correlated_plan_steps[node])
    {
        /// The rest of the query plan doesn't use any correlated columns.
        auto lhs_plan = context.query_plan.clone();

        const auto & settings = context.planner_context->getQueryContext()->getSettingsRef();

        auto lhs_plan_header = lhs_plan.getCurrentHeader();
        auto decorrelated_plan_header = node->step->getOutputHeader();

        ColumnsWithTypeAndName output_columns_and_types;
        output_columns_and_types.insert_range(output_columns_and_types.cend(), lhs_plan.getCurrentHeader().getColumnsWithTypeAndName());
        output_columns_and_types.insert_range(output_columns_and_types.cend(), node->step->getOutputHeader().getColumnsWithTypeAndName());

        JoinExpressionActions join_expression_actions(
            lhs_plan_header.getColumnsWithTypeAndName(),
            decorrelated_plan_header.getColumnsWithTypeAndName(),
            output_columns_and_types);

        Names output_columns;
        output_columns.insert_range(output_columns.cend(), lhs_plan.getCurrentHeader().getNames());
        output_columns.insert_range(output_columns.cend(), node->step->getOutputHeader().getNames());

        auto decorrelated_join = std::make_unique<JoinStepLogical>(
            lhs_plan_header,
            /*right_header_=*/decorrelated_plan_header,
            JoinInfo{
                .expression = {},
                .kind = JoinKind::Cross,
                .strictness = JoinStrictness::All,
                .locality = JoinLocality::Local
            },
            std::move(join_expression_actions),
            std::move(output_columns),
            settings[Setting::join_use_nulls],
            JoinSettings(settings),
            SortingStep::Settings(settings));
        decorrelated_join->setStepDescription("JOIN to evaluate correlated expression");

        /// Add CROSS JOIN
        QueryPlan result_plan;

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(lhs_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(context.correlated_query_plan.extractSubplan(node)));

        result_plan.unitePlans(std::move(decorrelated_join), {std::move(plans)});

        return result_plan;
    }

    if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());

        auto input_header = decorrelated_query_plan.getCurrentHeader();

        expression_step->decorrelateActions();
        expression_step->getExpression().appendInputsForUnusedColumns(input_header);
        for (const auto & column : input_header.getColumnsWithTypeAndName())
            expression_step->getExpression().tryRestoreColumn(column.name);

        expression_step->updateInputHeader(input_header);

        decorrelated_query_plan.addStep(std::move(node->step));
        return decorrelated_query_plan;
    }
    if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
    {
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        auto input_header = decorrelated_query_plan.getCurrentHeader();

        filter_step->decorrelateActions();
        filter_step->getExpression().appendInputsForUnusedColumns(input_header);
        for (const auto & column : input_header.getColumnsWithTypeAndName())
            filter_step->getExpression().tryRestoreColumn(column.name);

        node->step->updateInputHeader(input_header);

        decorrelated_query_plan.addStep(std::move(node->step));
        return decorrelated_query_plan;
    }
    if (auto * union_step = typeid_cast<UnionStep *>(node->step.get()))
    {
        auto decorrelated_lhs_plan = decorrelateQueryPlan(context, node->children.front());
        auto decorrelated_rhs_plan = decorrelateQueryPlan(context, node->children.back());

        Headers query_plans_headers{ decorrelated_lhs_plan.getCurrentHeader(), decorrelated_rhs_plan.getCurrentHeader() };

        std::vector<QueryPlanPtr> child_plans;
        child_plans.emplace_back(std::make_unique<QueryPlan>(std::move(decorrelated_lhs_plan)));
        child_plans.emplace_back(std::make_unique<QueryPlan>(std::move(decorrelated_rhs_plan)));

        Block union_common_header = buildCommonHeaderForUnion(query_plans_headers, SelectUnionMode::UNION_ALL); // Union mode doesn't matter here
        addConvertingToCommonHeaderActionsIfNeeded(child_plans, union_common_header, query_plans_headers);

        union_step->updateInputHeaders(std::move(query_plans_headers));

        QueryPlan result_plan;
        result_plan.unitePlans(std::move(node->step), std::move(child_plans));

        return result_plan;
    }
    if (auto * aggeregating_step = typeid_cast<AggregatingStep *>(node->step.get()))
    {
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        auto input_header = decorrelated_query_plan.getCurrentHeader();

        if (aggeregating_step->isGroupingSets())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Decorrelation of GROUP BY GROUPING SETS is not supported yet");

        auto new_aggregator_params = aggeregating_step->getAggregatorParameters();

        for (const auto & correlated_column_identifier : context.correlated_subquery.correlated_column_identifiers)
        {
            new_aggregator_params.keys.push_back(correlated_column_identifier);
        }
        new_aggregator_params.keys_size = new_aggregator_params.keys.size();

        auto result_step = std::make_unique<AggregatingStep>(
            std::move(input_header),
            std::move(new_aggregator_params),
            aggeregating_step->getGroupingSetsParamsList(),
            aggeregating_step->getFinal(),
            aggeregating_step->getMaxBlockSize(),
            aggeregating_step->getMaxBlockSizeForAggregationInOrder(),
            aggeregating_step->getMergeThreads(),
            aggeregating_step->getTemporaryDataMergeThreads(),
            false /*storage_has_evenly_distributed_read_*/,
            aggeregating_step->isGroupByUseNulls(),
            SortDescription{} /*sort_description_for_merging_*/,
            SortDescription{} /*group_by_sort_description_*/,
            aggeregating_step->shouldProduceResultsInBucketOrder(),
            aggeregating_step->usingMemoryBoundMerging(),
            aggeregating_step->explicitSortingRequired()
        );
        result_step->setStepDescription(aggeregating_step->getStepDescription());

        decorrelated_query_plan.addStep(std::move(result_step));

        return decorrelated_query_plan;
    }
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot decorrelate query, because '{}' step is not supported",
        node->step->getName());
}

void buildRenamingForScalarSubquery(
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery
)
{
    ActionsDAG dag(query_plan.getCurrentHeader().getNamesAndTypesList());
    const auto * result_node = &dag.findInOutputs(correlated_subquery.action_node_name);

    ActionsDAG::NodeRawConstPtrs new_outputs{ result_node };
    new_outputs.reserve(correlated_subquery.correlated_column_identifiers.size() + 1);

    for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
    {
        new_outputs.push_back(&dag.addAlias(dag.findInOutputs(column_name), fmt::format("{}.{}", correlated_subquery.action_node_name, column_name)));
    }
    new_outputs.push_back(result_node);

    dag.getOutputs() = std::move(new_outputs);

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create renaming actions for scalar subquery");
    query_plan.addStep(std::move(expression_step));
}

void buildExistsResultExpression(
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    bool project_only_correlated_columns
)
{
    ActionsDAG dag(query_plan.getCurrentHeader().getNamesAndTypesList());
    auto result_type = std::make_shared<DataTypeUInt8>();
    auto column = result_type->createColumnConst(1, 1);
    const auto * exists_result = &dag.materializeNode(dag.addColumn(ColumnWithTypeAndName(column, result_type, correlated_subquery.action_node_name)));

    if (project_only_correlated_columns)
    {
        ActionsDAG::NodeRawConstPtrs new_outputs;
        new_outputs.reserve(correlated_subquery.correlated_column_identifiers.size() + 1);

        for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
        {
            new_outputs.push_back(&dag.addAlias(dag.findInOutputs(column_name), fmt::format("{}.{}", correlated_subquery.action_node_name, column_name)));
        }
        new_outputs.push_back(exists_result);

        dag.getOutputs() = std::move(new_outputs);
    }
    else
    {
        dag.addOrReplaceInOutputs(*exists_result);
    }

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create result for always true EXISTS expression");
    query_plan.addStep(std::move(expression_step));
}

/// Remove query plan steps that don't affect the number of rows in the result.
/// Returns true if the query always returns at least 1 row.
bool optimizeCorrelatedPlanForExists(QueryPlan & correlated_query_plan)
{
    auto * node = correlated_query_plan.getRootNode();
    while (true)
    {
        if (typeid_cast<ExpressionStep *>(node->step.get()))
        {
            node = node->children[0];
            continue;
        }
        if (auto * aggregation = typeid_cast<AggregatingStep *>(node->step.get()))
        {
            const auto & params = aggregation->getParams();
            if (params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
            {
                /// Subquery will always produce at least one row
                return true;
            }
            node = node->children[0];
            continue;
        }
        break;
    }

    if (node != correlated_query_plan.getRootNode())
    {
        correlated_query_plan = correlated_query_plan.extractSubplan(node);
    }
    return false;
}

QueryPlan buildLogicalJoin(
    const PlannerContextPtr & planner_context,
    QueryPlan left_plan,
    QueryPlan right_plan,
    const CorrelatedSubquery & correlated_subquery
)
{
    const auto & lhs_plan_header = left_plan.getCurrentHeader();
    const auto & rhs_plan_header = right_plan.getCurrentHeader();

    ColumnsWithTypeAndName output_columns_and_types;
    output_columns_and_types.insert_range(output_columns_and_types.cend(), lhs_plan_header.getColumnsWithTypeAndName());
    output_columns_and_types.emplace_back(rhs_plan_header.getByName(correlated_subquery.action_node_name));

    JoinExpressionActions join_expression_actions(
        lhs_plan_header.getColumnsWithTypeAndName(),
        rhs_plan_header.getColumnsWithTypeAndName(),
        output_columns_and_types);

    Names output_columns;
    output_columns.insert_range(output_columns.cend(), lhs_plan_header.getNames());
    output_columns.push_back(correlated_subquery.action_node_name);

    const auto & settings = planner_context->getQueryContext()->getSettingsRef();

    std::vector<JoinPredicate> predicates;
    for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
    {
        const auto * left_node = &join_expression_actions.left_pre_join_actions->findInOutputs(column_name);
        const auto * right_node = &join_expression_actions.right_pre_join_actions->findInOutputs(fmt::format("{}.{}", correlated_subquery.action_node_name, column_name));

        JoinPredicate predicate{
            .left_node = JoinActionRef(left_node, join_expression_actions.left_pre_join_actions.get()),
            .right_node = JoinActionRef(right_node, join_expression_actions.right_pre_join_actions.get()),
            .op = PredicateOperator::Equals
        };

        predicates.emplace_back(std::move(predicate));
    }

    /// Add LEFT OUTER JOIN
    auto result_join = std::make_unique<JoinStepLogical>(
        lhs_plan_header,
        rhs_plan_header,
        JoinInfo{
            .expression = JoinExpression{
                .condition = JoinCondition{
                    .predicates = std::move(predicates),
                    .left_filter_conditions = {},
                    .right_filter_conditions = {},
                    .residual_conditions = {}
                },
                .disjunctive_conditions = {}
            },
            .kind = JoinKind::Left,
            .strictness = JoinStrictness::Any,
            .locality = JoinLocality::Local
        },
        std::move(join_expression_actions),
        std::move(output_columns),
        /*join_use_nulls=*/false,
        JoinSettings(settings),
        SortingStep::Settings(settings));
    result_join->setStepDescription("JOIN to generate result stream");

    QueryPlan result_plan;

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

    result_plan.unitePlans(std::move(result_join), {std::move(plans)});
    return result_plan;
}

Planner buildPlannerForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options
)
{
    auto subquery_options = select_query_options.subquery();
    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
    /// Register table expression data for correlated columns sources in the global context.
    /// Table expression data would be reused because it can't be initialized
    /// during plan construction for correlated subquery.
    global_planner_context->collectTableExpressionDataForCorrelatedColumns(correlated_subquery.query_tree, planner_context);

    Planner subquery_planner(
        correlated_subquery.query_tree,
        subquery_options,
        std::move(global_planner_context));
    subquery_planner.buildQueryPlanIfNeeded();

    return subquery_planner;
}

void addStepForResultRenaming(
    const CorrelatedSubquery & correlated_subquery,
    QueryPlan & correlated_subquery_plan
)
{
    const auto & header = correlated_subquery_plan.getCurrentHeader();
    const auto & subquery_result_columns = header.getColumnsWithTypeAndName();

    if (subquery_result_columns.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected to get only 1 result column of correlated subquery, but got {}",
            subquery_result_columns.size());

    const auto & result_column = subquery_result_columns[0];
    auto expected_result_type = correlated_subquery.query_tree->getResultType();
    if (!expected_result_type->equals(*result_column.type))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected {} as correlated subquery result, but got {}",
            expected_result_type->getName(),
            result_column.type->getName());

    ActionsDAG dag(subquery_result_columns);

    const auto * alias_node = &dag.addAlias(*dag.getOutputs()[0], correlated_subquery.action_node_name);
    dag.getOutputs() = { alias_node };

    auto expression_step = std::make_unique<ExpressionStep>(header, std::move(dag));
    expression_step->setStepDescription("Create correlated subquery result alias");
    correlated_subquery_plan.addStep(std::move(expression_step));
}

}

/* Build query plan for correlated subquery using decorrelation algorithm
 * on top of relational algebra operators proposed by TU Munich researchers
 * Thomas Neumann and Alfons Kemper.
 *
 * Original research paper "Unnesting Arbitrary Queries": https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf
 * See also a follow-up paper, "Improving Unnesting of Complex Queries": https://dl.gi.de/items/b9df4765-d1b0-4267-a77c-4ce4ab0ee62d
 *
 * NOTE: ClickHouse does not explicitly build SQL query into relational algebra expression.
 * Instead, it produces a query plan where almost every step has an analog from relational algebra.
 * This function implements a decorrelation algorithm using the ClickHouse query plan.
 *
 * TODO: Support scalar correlated subqueries.
 * TODO: Support decorrelation of all kinds of query plan steps.
 * TODO: Implement left table substitution optimization: T_left DEPENDENT JOIN T_right is a subset of T_right
 * if T_right has all the necessary columns of T_left.
 */
void buildQueryPlanForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options)
{
    auto * query_node = correlated_subquery.query_tree->as<QueryNode>();
    auto * union_node = correlated_subquery.query_tree->as<UnionNode>();
    chassert(query_node != nullptr && query_node->isCorrelated() || union_node != nullptr && union_node->isCorrelated());

    switch (correlated_subquery.kind)
    {
        case DB::CorrelatedSubqueryKind::SCALAR:
        {
            Planner subquery_planner = buildPlannerForCorrelatedSubquery(planner_context, correlated_subquery, select_query_options);
            /// Logical plan for correlated subquery
            auto & correlated_query_plan = subquery_planner.getQueryPlan();

            addStepForResultRenaming(correlated_subquery, correlated_query_plan);

            /// Mark all query plan steps if they or their subplans contain usage of correlated subqueries.
            /// It's needed to identify the moment when dependent join can be replaced by CROSS JOIN.
            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            DecorrelationContext context{
                .correlated_subquery = correlated_subquery,
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(subquery_planner).extractQueryPlan(),
                .correlated_plan_steps = std::move(correlated_step_map)
            };

            auto decorrelated_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            buildRenamingForScalarSubquery(decorrelated_plan, correlated_subquery);

            /// Use LEFT OUTER JOIN to produce the result plan.
            query_plan = buildLogicalJoin(
                planner_context,
                std::move(context.query_plan),
                std::move(decorrelated_plan),
                correlated_subquery);
            break;
        }
        case CorrelatedSubqueryKind::EXISTS:
        {
            Planner subquery_planner = buildPlannerForCorrelatedSubquery(planner_context, correlated_subquery, select_query_options);
            /// Logical plan for correlated subquery
            auto & correlated_query_plan = subquery_planner.getQueryPlan();

            /// For EXISTS expression we can remove plan steps that doesn't change the number of result rows.
            /// It may also result in non-correlated subquery plan
            /// Example:
            /// SELECT * FROM numbers(1) WHERE EXISTS (SELECT a = number FROM table)
            if (optimizeCorrelatedPlanForExists(correlated_query_plan))
            {
                /// Subquery always produces at least 1 row.
                buildExistsResultExpression(query_plan, correlated_subquery, /*project_only_correlated_columns=*/false);
                return;
            }

            /// Mark all query plan steps if they or their subplans contain usage of correlated subqueries.
            /// It's needed to identify the moment when dependent join can be replaced by CROSS JOIN.
            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            DecorrelationContext context{
                .correlated_subquery = correlated_subquery,
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(subquery_planner).extractQueryPlan(),
                .correlated_plan_steps = std::move(correlated_step_map)
            };

            auto decorrelated_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            /// Add a 'exists(<table expression id>)' expression that is always true.
            buildExistsResultExpression(decorrelated_plan, correlated_subquery, /*project_only_correlated_columns=*/true);

            /// Use LEFT OUTER JOIN to produce the result plan.
            /// If there's no corresponding rows from the right side, 'exists(<table expression id>)' would be replaced by default value (false).
            query_plan = buildLogicalJoin(
                planner_context,
                std::move(context.query_plan),
                std::move(decorrelated_plan),
                correlated_subquery);
            break;
        }
    }
}

}
