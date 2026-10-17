#pragma once

#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/CapturedPlan.h>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>
#include <base/types.h>

#include <cstddef>


namespace DB
{

/// Written into every plan as `Version`, so a reader can tell what it is looking at rather than
/// guessing from which keys happen to be present.
///
/// Bump only when a reader written against the previous version would misread this one -- a key
/// renamed, removed, or given a different meaning. Adding a key is not such a change: a reader
/// that does not know it ignores it, and rows written by older servers stay readable either way,
/// because `system.query_log` holds whatever version was current when each row was written.
constexpr UInt64 QUERY_PLAN_JSON_VERSION = 1;

/// Writes the document.
JSONBuilder::ItemPtr capturedPlanToJSON(const CapturedPlan & captured);

}
