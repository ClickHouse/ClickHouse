#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

namespace DB
{

/// `cancellation` is handed to every source created; see `StreamingExchangeSource`. Null on a worker.
ExchangeLookupPtr createStreamingExchangeLookup(
    const String & query_id,
    ExchangeConnectionsPtr connections,
    const ExchangeStreamSources & exchange_stream_sources,
    DistributedQueryCancellationPtr cancellation);

}

#endif
