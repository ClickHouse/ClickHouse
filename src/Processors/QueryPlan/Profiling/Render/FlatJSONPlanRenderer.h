#pragma once

#include <Processors/QueryPlan/Profiling/Render/IPlanRenderer.h>
#include <base/types.h>

namespace DB
{

/// Written into every plan as `Version`, so a reader can tell what it is looking at rather than
/// guessing from which keys happen to be present.
///
/// Bump only when a reader written against the previous version would misread this one -- a key
/// renamed, removed, or given a different meaning. Adding a key is not such a change: a reader
/// that does not know it ignores it, and rows written by older servers stay readable either way,
/// because `system.query_log` holds whatever version was current when each row was written.
constexpr UInt64 QUERY_PLAN_JSON_VERSION = 2;

/// Writes the captured plan as the JSON stored in `system.query_log.query_plan`.
///
/// The whole document is built here: `{Version, Plans[]}`, with one entry of `Plans` for every plan
/// that ran for the query, each holding its own flat `Nodes` array and the `Statistics` of each
/// step. The query's own plan is the first entry, of `Kind` `Query`; a step that reads the result
/// of another entry lists its `SubqueryId` under `SubPlans`. Values are written raw -- nanoseconds, rows and bytes as numbers,
/// never as `4.04 ms` or `142.86 thousand` -- because the column is meant to be aggregated by a
/// query, which formatted values would prevent.
class FlatJSONPlanRenderer : public IPlanRenderer
{
public:
    void render(const CapturedPlan & captured, WriteBuffer & out) override;

    /// A document carrying only the reason the plan could not be written, so that a row says what
    /// went wrong rather than holding a truncated plan.
    static void renderError(const String & message, WriteBuffer & out);
};

}
