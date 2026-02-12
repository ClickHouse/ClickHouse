#pragma once

#ifdef OS_LINUX

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

namespace DB
{

ExchangeLookupPtr createStreamingExchangeLookup(
    const String & query_id,
    ExchangeConnectionsPtr connections,
    const ExchangeStreamSources & exchange_stream_sources,
    UInt16 streaming_exchange_port);

}

#endif
