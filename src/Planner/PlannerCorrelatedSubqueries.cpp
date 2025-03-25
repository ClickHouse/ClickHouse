#include <algorithm>
#include <memory>
#include <ranges>
#include <string_view>
#include <Planner/PlannerCorrelatedSubqueries.h>
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
    LOG_DEBUG(getLogger(__func__), "Decorrelating step: {}({})\n{}", node->step->getName(), node->step->getStepDescription(), node->step->getOutputHeader().dumpNames());
    LOG_DEBUG(getLogger(__func__), "Has correlated expressions: {}", context.correlated_plan_steps[node]);

    if (!context.correlated_plan_steps[node])
    {
        LOG_DEBUG(getLogger(__func__), "Clonning LHS");

        auto lhs_plan = context.query_plan.clone();

        LOG_DEBUG(getLogger(__func__), "Clonned LHS");
        LOG_DEBUG(getLogger(__func__), "Clonned LHS:\n{}", dumpQueryPlan(lhs_plan));

        const auto & settings = context.planner_context->getQueryContext()->getSettingsRef();

        auto lhs_plan_header = lhs_plan.getCurrentHeader();
        auto decorrelated_plan_header = node->step->getOutputHeader();

        ColumnsWithTypeAndName output_columns_and_types;
        output_columns_and_types.insert_range(output_columns_and_types.cend(), lhs_plan.getCurrentHeader().getColumnsWithTypeAndName());
        output_columns_and_types.insert_range(output_columns_and_types.cend(), node->step->getOutputHeader().getColumnsWithTypeAndName());

        LOG_DEBUG(getLogger(__func__), "Building JoinExpressionActions");
        JoinExpressionActions join_expression_actions(
            lhs_plan_header.getColumnsWithTypeAndName(),
            decorrelated_plan_header.getColumnsWithTypeAndName(),
            output_columns_and_types);

        Names output_columns;
        output_columns.insert_range(output_columns.cend(), lhs_plan.getCurrentHeader().getNames());
        output_columns.insert_range(output_columns.cend(), node->step->getOutputHeader().getNames());

        LOG_DEBUG(getLogger(__func__), "Building JoinStepLogical");
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
        /// Add CROSS JOIN

        LOG_DEBUG(getLogger(__func__), "Built JOIN step");

        LOG_DEBUG(getLogger(__func__), "Built JOIN step header:\n{}", decorrelated_join->getOutputHeader().dumpStructure());

        QueryPlan result_plan;

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(lhs_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(context.correlated_query_plan.extractSubplan(node)));

        LOG_DEBUG(getLogger(__func__), "Going to unite plans with JOIN");

        result_plan.unitePlans(std::move(decorrelated_join), {std::move(plans)});
        LOG_DEBUG(getLogger(__func__), "Built decorrelated JOIN:\n{}", dumpQueryPlan(result_plan));

        return result_plan;
    }

    if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        LOG_DEBUG(getLogger(__func__), "Decorrelating child plan for ExpressionStep");
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        expression_step->decorrelateActions();
        // expression_step->updateInputHeader(decorrelated_query_plan.getCurrentHeader());
        decorrelated_query_plan.addStep(std::move(node->step));
        LOG_DEBUG(getLogger(__func__), "Decorrelated child plan for ExpressionStep");
        LOG_DEBUG(getLogger(__func__), "Plan after ExpressionStep:\n{}", dumpQueryPlan(decorrelated_query_plan));
        return decorrelated_query_plan;
    }
    if ([[maybe_unused]] auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
    {
        LOG_DEBUG(getLogger(__func__), "Decorrelating child plan for FilterStep");
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        LOG_DEBUG(getLogger(__func__), "Decorrelated child plan for FilterStep:\n{}", dumpQueryPlan(decorrelated_query_plan));
        LOG_DEBUG(getLogger(__func__), "FilterStep1: {}", node->step->getStepDescription());
        LOG_DEBUG(getLogger(__func__), "FilterStep: {}", filter_step->getStepDescription());

        filter_step->decorrelateActions();
        LOG_DEBUG(getLogger(__func__), "Decorrelated actions for FilterStep");
        LOG_DEBUG(getLogger(__func__), "Decorrelated actions for FilterStep\n{}", decorrelated_query_plan.getCurrentHeader().dumpStructure());

        node->step->updateInputHeader(decorrelated_query_plan.getCurrentHeader());
        LOG_DEBUG(getLogger(__func__), "Updated header for FilterStep");
        decorrelated_query_plan.addStep(std::move(node->step));
        LOG_DEBUG(getLogger(__func__), "Plan after FilterStep:\n{}", dumpQueryPlan(decorrelated_query_plan));
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
    ActionsDAG dag;
    auto result_type = std::make_shared<DataTypeUInt8>();
    auto column = result_type->createColumnConst(1, 1);
    dag.addColumn(ColumnWithTypeAndName(column, result_type, correlated_subquery.action_node_name));

    dag.appendInputsForUnusedColumns(query_plan.getCurrentHeader());

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create result for always true EXISTS expression");
    query_plan.addStep(std::move(expression_step));
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

    std::vector<ColumnIdentifier> correlated_column_identifiers;
    for (const auto & column : query_node->getCorrelatedColumns())
    {
        correlated_column_identifiers.push_back(planner_context->getColumnNodeIdentifierOrThrow(column));
    }
    LOG_DEBUG(getLogger(__func__), "Correlated Identifiers:\n{}", fmt::join(correlated_column_identifiers, ", "));

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
                        buildPlanForAlwaysExists(query_plan, correlated_subquery);
                        return;
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

            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            LOG_DEBUG(getLogger(__func__), "Correlated step map is built");

            DecorrelationContext context{
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(subquery_planner).extractQueryPlan(),
                .correlated_plan_steps = std::move(correlated_step_map)
            };
            query_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            LOG_DEBUG(getLogger(__func__), "Decorrelated plan:\n{}", dumpQueryPlan(query_plan));
            break;
        }
    }
}

}
