#pragma once

#include <Core/SortDescription.h>

#include <Planner/PlannerContext.h>

namespace DB
{

/// Extract sort description from order by node
SortDescription extractSortDescription(const QueryTreeNodePtr & order_by_node, const PlannerContext & planner_context);

}

