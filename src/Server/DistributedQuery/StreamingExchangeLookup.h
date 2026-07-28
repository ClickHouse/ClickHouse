#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

namespace DB
{

ExchangeLookupPtr createStreamingExchangeLookup(
    const String & query_id,
    ExchangeConnectionsPtr connections,
    const ExchangeStreamSources & exchange_stream_sources);

}

#endif
