#pragma once

#include <Processors/QueryPlan/Profiling/Render/IPlanRenderer.h>

namespace DB
{

/// Writes the captured plan as the JSON stored in `system.query_log.query_plan`.
///
/// Values are written raw -- nanoseconds, rows and bytes as numbers, never as `4.04 ms` or
/// `142.86 thousand` -- because the column is meant to be aggregated by a query, which formatted
/// values would prevent.
class JsonPlanRenderer : public IPlanRenderer
{
public:
    void render(const CapturedPlan & captured, WriteBuffer & out) override;
};

}
