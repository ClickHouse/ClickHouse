#pragma once

#include <Core/SortDescription.h>

#include <Planner/PlannerContext.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

/// Extract window descriptions from window function nodes
std::vector<WindowDescription> extractWindowDescriptions(const QueryTreeNodes & window_function_nodes, const PlannerContext & planner_context);

/** Try to sort window descriptions in such an order that the window with the longest
  * sort description goes first, and all window that use its prefixes follow.
  */
void sortWindowDescriptions(std::vector<WindowDescription> & window_descriptions);

}
