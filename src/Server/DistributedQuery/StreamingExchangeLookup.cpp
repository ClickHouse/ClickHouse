#ifdef OS_LINUX

#include <Server/DistributedQuery/StreamingExchangeLookup.h>
#include <Server/DistributedQuery/StreamingExchangeSink.h>
#include <Server/DistributedQuery/StreamingExchangeSource.h>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class StreamingExchangeLookup : public IExchangeLookup
{
public:
    explicit StreamingExchangeLookup(
        const String & query_id_,
        ExchangeConnectionsPtr connections_,
        const ExchangeStreamSources & exchange_stream_sources_,
        UInt16 streaming_exchange_port_)
        : query_id(query_id_)
        , connections(connections_)
        , exchange_stream_sources(exchange_stream_sources_)
        , streaming_exchange_port(streaming_exchange_port_)
    {
    }

    std::shared_ptr<ISink> createSink(SharedHeader input_header, const ExchangeStreamId & exchange_stream_id) override
    {
        auto stream_name = exchange_stream_id.toString();
        auto future_connection = connections->getConnection(query_id, stream_name);
        return std::make_shared<StreamingExchangeSink>(input_header, future_connection, stream_name);
    }

    std::shared_ptr<ISource> createSource(SharedHeader output_header, const ExchangeStreamId & exchange_stream_id) override
    {
        auto stream_name = exchange_stream_id.toString();
        auto it = exchange_stream_sources.stream_hosts.find(stream_name);
        if (it == exchange_stream_sources.stream_hosts.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No host found for exchange stream {}", stream_name);
        return std::make_shared<StreamingExchangeSource>(output_header, query_id, stream_name, it->second, streaming_exchange_port);
    }

private:
    const String query_id;
    const ExchangeConnectionsPtr connections;
    const ExchangeStreamSources exchange_stream_sources;
    const UInt16 streaming_exchange_port;
};

ExchangeLookupPtr createStreamingExchangeLookup(
    const String & query_id,
    ExchangeConnectionsPtr connections,
    const ExchangeStreamSources & exchange_stream_sources,
    UInt16 streaming_exchange_port)
{
    return std::make_shared<StreamingExchangeLookup>(query_id, connections, exchange_stream_sources, streaming_exchange_port);
}

}

#endif
