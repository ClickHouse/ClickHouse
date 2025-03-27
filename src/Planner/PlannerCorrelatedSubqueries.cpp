#include <algorithm>
#include <memory>
#include <ranges>
#include <string_view>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include <fmt/format.h>
#include "Common/Exception.h"
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include "Common/typeid_cast.h"
#include "Analyzer/FunctionNode.h"
#include "Analyzer/QueryNode.h"
#include "Core/Joins.h"
#include "Core/Settings.h"
#include "DataTypes/DataTypesNumber.h"
#include "IO/ReadBufferFromString.h"
#include "IO/WriteBufferFromString.h"
#include "Interpreters/ActionsDAG.h"
#include "Interpreters/JoinInfo.h"
#include "Planner/Planner.h"
#include "Planner/PlannerActionsVisitor.h"
#include "Planner/PlannerContext.h"
#include "Planner/PlannerJoinsLogical.h"
#include "Planner/TableExpressionData.h"
#include "Planner/Utils.h"
#include "Processors/QueryPlan/AggregatingStep.h"
#include "Processors/QueryPlan/ExpressionStep.h"
#include "Processors/QueryPlan/FilterStep.h"
#include "Processors/QueryPlan/JoinStepLogical.h"
#include "Processors/QueryPlan/QueryPlan.h"
#include "base/defines.h"

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;

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
        auto & current = nodes_to_process.back();
        if (current.processed_children)
        {
            auto & value = result[current.node];
            value = current.node->step->hasCorrelatedExpressions();

            for (auto * child : current.node->children)
                value |= result[child];

            nodes_to_process.pop_back();
        }
        else
        {
            for (auto * child : current.node->children)
                nodes_to_process.push_back({ .node = child });
            current.processed_children = true;
        }
    }

    return result;
}

struct DecorrelationContext
{
    const PlannerContextPtr & planner_context;
    QueryPlan query_plan; // LHS plan
    QueryPlan correlated_query_plan;
    CorrelatedPlanStepMap correlated_plan_steps;
};

QueryPlan decorrelateQueryPlan(
    DecorrelationContext & context,
    QueryPlan::Node * node
)
{
    if (!context.correlated_plan_steps[node])
    {
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
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot decorrelate query, because '{}' step is not supported",
        node->step->getStepDescription());
}

void buildPlanForAlwaysExists(
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery
)
{
    ActionsDAG dag(query_plan.getCurrentHeader().getNamesAndTypesList());
    auto result_type = std::make_shared<DataTypeUInt8>();
    auto column = result_type->createColumnConst(1, 1);
    const auto * exists_result = &dag.addColumn(ColumnWithTypeAndName(column, result_type, correlated_subquery.action_node_name));
    dag.addOrReplaceInOutputs(*exists_result);

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create result for always true EXISTS expression");
    query_plan.addStep(std::move(expression_step));
}

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
        LOG_DEBUG(
            getLogger(__func__),
            "Simplified correlated subquery plan for EXISTS:\n{}",
            dumpQueryPlan(correlated_query_plan));
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
        const auto * right_node = &join_expression_actions.right_pre_join_actions->findInOutputs(column_name);

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
            .strictness = JoinStrictness::All,
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

}

void buildQueryPlanForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options)
{
    auto * query_node = correlated_subquery.query_tree->as<QueryNode>();
    chassert(query_node->isCorrelated());

    LOG_DEBUG(getLogger(__func__), "Planning:\n{}", correlated_subquery.query_tree->dumpTree());

    LOG_DEBUG(getLogger(__func__), "Correlated Identifiers:\n{}", fmt::join(correlated_subquery.correlated_column_identifiers, ", "));

    switch (correlated_subquery.kind)
    {
        case DB::CorrelatedSubqueryKind::SCALAR:
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Scalar correlated subqueries are not supported");
        }
        case CorrelatedSubqueryKind::EXISTS:
        {
            auto subquery_options = select_query_options.subquery();
            Planner subquery_planner(
                correlated_subquery.query_tree,
                subquery_options,
                std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{}));

            subquery_planner.buildQueryPlanIfNeeded();
            auto & correlated_query_plan = subquery_planner.getQueryPlan();
            LOG_DEBUG(getLogger(__func__), "Correlated subquery plan:\n{}", dumpQueryPlan(correlated_query_plan));

            if (optimizeCorrelatedPlanForExists(correlated_query_plan))
            {
                buildPlanForAlwaysExists(query_plan, correlated_subquery);
                return;
            }

            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            DecorrelationContext context{
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(subquery_planner).extractQueryPlan(),
                .correlated_plan_steps = std::move(correlated_step_map)
            };

            auto decorrelated_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            buildPlanForAlwaysExists(decorrelated_plan, correlated_subquery);
            LOG_DEBUG(getLogger(__func__), "Decorrelated plan for subquery:\n{}", dumpQueryPlan(decorrelated_plan));

            query_plan = buildLogicalJoin(
                planner_context,
                std::move(context.query_plan),
                std::move(decorrelated_plan),
                correlated_subquery);
            LOG_DEBUG(getLogger(__func__), "Decorrelated plan:\n{}", dumpQueryPlan(query_plan));
            break;
        }
    }
}

}
