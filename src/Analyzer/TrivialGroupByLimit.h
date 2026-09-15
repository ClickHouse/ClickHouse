#pragma once

#include <base/types.h>

#include <optional>

namespace DB
{

class QueryNode;
struct Settings;

/// Checks whether a query has the trivial `GROUP BY ... LIMIT` shape eligible for the
/// optimization controlled by the `optimize_trivial_group_by_limit_query` setting:
/// a plain GROUP BY with a constant non-negative LIMIT and nothing that consumes or
/// filters the groups between the aggregation and the LIMIT (HAVING, ORDER BY, WINDOW,
/// QUALIFY, LIMIT BY, DISTINCT, GROUP BY modifiers, window functions or `arrayJoin`
/// in the projection).
///
/// For such queries the aggregation may keep only the first `LIMIT + OFFSET` distinct
/// keys: an unspecified subset of the groups is a valid result for LIMIT without ORDER BY.
///
/// Returns `LIMIT + OFFSET` when the shape matches, the value is non-zero and does not
/// overflow, and the user has not set a non-`ANY` `group_by_overflow_mode` (which the
/// optimization would silently override). Comparing the result against a user-set
/// `max_rows_to_group_by` is left to the callers because they need different comparisons
/// (see `OptimizeTrivialGroupByLimitPass` and `addAggregationStep` in the planner).
std::optional<UInt64> getTrivialGroupByLimit(const QueryNode & query, const Settings & settings);

}
