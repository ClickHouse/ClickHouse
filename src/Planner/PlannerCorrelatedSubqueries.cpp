#include <algorithm>
#include <ranges>
#include <string_view>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include "Common/Exception.h"
#include "Common/logger_useful.h"
#include "Common/typeid_cast.h"
#include "Analyzer/FunctionNode.h"
#include "Analyzer/QueryNode.h"
#include "Planner/Planner.h"
#include "Planner/PlannerActionsVisitor.h"
#include "Planner/PlannerContext.h"
#include "Planner/TableExpressionData.h"
#include "Planner/Utils.h"
#include "Processors/QueryPlan/ExpressionStep.h"
#include "Processors/QueryPlan/FilterStep.h"
#include "base/defines.h"

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;

}

void CorrelatedSubtrees::assertEmpty(std::string_view reason) const
{
    if (notEmpty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Correlated subqueries {} are not supported", reason);
}

namespace
{

void decorrelatedQueryPlan(QueryPlan::Node * node)
{
    if ([[maybe_unused]] auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        decorrelatedQueryPlan(node->children.front());
    }
    if ([[maybe_unused]] auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
    {

    }
}

}

void buildQueryPlanForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
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
            decorrelatedQueryPlan(subquery_planner.getQueryPlan().getRootNode());
            LOG_DEBUG(getLogger(__func__), "Correlated subquery plan:\n{}", dumpQueryPlan(subquery_planner.getQueryPlan()));
            break;
        }
    }
}

}
