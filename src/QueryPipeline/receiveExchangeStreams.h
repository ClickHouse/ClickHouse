#pragma once

#include <Core/Block_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/QueryPlan/ExchangeLookup.h>


namespace DB
{

class QueryPipelineBuilder;
struct BuildQueryPipelineSettings;

/// Builds the receiving side of an exchange: one source per stream of `stream_ids` and, for an
/// exchange kind whose sources hand out packets, a deserializer behind every source. With
/// `spread_over_max_threads` the packets are first spread over `max_threads` streams, so the
/// deserialization and the steps after it run on all threads. A receive that has to keep the order
/// of every stream passes false and gets one stream per source.
QueryPipelineBuilder receiveExchangeStreams(
    const SharedHeader & output_header,
    const String & exchange_id,
    const VectorWithMemoryTracking<ExchangeStreamId> & stream_ids,
    const BuildQueryPipelineSettings & settings,
    bool spread_over_max_threads);

}
