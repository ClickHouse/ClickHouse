#include <string_view>
#include <Planner/PlannerCorrelatedSubqueries.h>
#include "Common/Exception.h"
#include "Common/logger_useful.h"
#include "Analyzer/FunctionNode.h"
#include "Planner/Planner.h"
#include "Planner/PlannerContext.h"
#include "Planner/Utils.h"

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

void buildQueryPlanForCorrelatedSubquery(
    const QueryTreeNodePtr & correlated_subquery,
    const SelectQueryOptions & select_query_options)
{
    LOG_DEBUG(getLogger(__func__), "Planning:\n{}", correlated_subquery->dumpTree());

    if (auto * function_node = correlated_subquery->as<FunctionNode>())
    {
        if (function_node->getFunctionName() == "exists")
        {
            auto actual_subquery = function_node->getArguments().getNodes()[0];

            auto subquery_options = select_query_options.subquery();
            Planner subquery_planner(
                actual_subquery,
                subquery_options,
                std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{}));

            subquery_planner.buildQueryPlanIfNeeded();
            LOG_DEBUG(getLogger(__func__), "Correlated subquery plan:\n{}", dumpQueryPlan(subquery_planner.getQueryPlan()));
        }
    }
}

}
