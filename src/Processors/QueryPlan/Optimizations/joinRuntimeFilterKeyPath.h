#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Whether a filter on `column`, pushed down into `node` as `tryPushDownFilter` does, reaches a join input estimated above `limit` rows.
bool keyPathHasInputAbove(const QueryPlan::Node * node, const String & column, UInt64 limit);

}
