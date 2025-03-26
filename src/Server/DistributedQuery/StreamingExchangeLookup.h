#pragma once

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Processors/QueryPlan/ExchangeLookup.h>

namespace DB
{

ExchangeLookupPtr createStreamingExchangeLookup(const String & query_id, ExchangeConnectionsPtr connections, UInt16 streaming_exchange_port);

}
