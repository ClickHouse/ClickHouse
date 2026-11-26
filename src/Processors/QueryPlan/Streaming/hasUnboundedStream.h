#pragma once

namespace DB
{

class QueryPlan;

/// Whether the plan reads a `STREAM` subscription for updates, which makes the query a streaming query.
/// Other sources that never end, such as `system.numbers`, do not make it streaming. Once the pipeline is
/// built, `QueryPipelineBuilder::isUnbounded` tells whether a stream may never end, whatever its source.
bool hasUnboundedStream(const QueryPlan & query_plan);

}
