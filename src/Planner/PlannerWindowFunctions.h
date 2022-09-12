#pragma once

#include <Core/SortDescription.h>

#include <Planner/PlannerContext.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

/// Extract and sort window description from query.
std::vector<WindowDescription> extractWindowDescriptions(const QueryTreeNodes & window_function_nodes, const PlannerContext & planner_context);

/** Try to sort window description in such an order that the window with the longest
  * sort description goes first, and all window that use its prefixes follow.
  */
void sortWindowDescriptions(std::vector<WindowDescription> & window_descriptions);

}
