#pragma once

#include <Core/SortDescription.h>

#include <Planner/PlannerContext.h>

namespace DB
{

/// Extract sort description from order by node
SortDescription extractSortDescription(const QueryTreeNodePtr & order_by_node, const PlannerContext & planner_context);

/// Holds the column names needed for ORDER BY x DEPENDS ON deps topological sort
struct TopologicalSortInfo
{
    String key_column_name;
    String deps_column_name;
};

/// Returns TopologicalSortInfo if the order by list has a single DEPENDS ON element,
/// otherwise returns std::nullopt.
std::optional<TopologicalSortInfo> extractTopologicalSortInfo(
    const QueryTreeNodePtr & order_by_node, const PlannerContext & planner_context);

}

