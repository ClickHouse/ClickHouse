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

class StreamingExchangeLookup : public IExchangeLookup
{
public:
    explicit StreamingExchangeLookup(
        const String & query_id_,
        ExchangeConnectionsPtr connections_,
        const ExchangeStreamDestinations & exchange_stream_destinations_,
        UInt16 streaming_exchange_port_)
        : query_id(query_id_)
        , connections(connections_)
        , exchange_stream_destinations(exchange_stream_destinations_)
        , streaming_exchange_port(streaming_exchange_port_)
    {
    }

    std::shared_ptr<ISink> createSink(const Header & input_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto stream_name = streamNameForExchange(exchange_id, source_bucket_id, destination_bucket_id);
        String host = exchange_stream_destinations.stream_hosts.at(stream_name);
        return std::make_shared<StreamingExchangeSink>(input_header, query_id, stream_name, host, streaming_exchange_port);
    }

    std::shared_ptr<ISource> createSource(const Header & output_header, const String & exchange_id, const String & source_bucket_id, const String & destination_bucket_id) override
    {
        auto stream_name = streamNameForExchange(exchange_id, source_bucket_id, destination_bucket_id);
        auto socket = connections->getConnection(query_id, stream_name);
        return std::make_shared<StreamingExchangeSource>(output_header, socket, stream_name);
    }

private:
    const String query_id;
    const ExchangeConnectionsPtr connections;
    const ExchangeStreamDestinations exchange_stream_destinations;
    const UInt16 streaming_exchange_port;
};

ExchangeLookupPtr createStreamingExchangeLookup(
    const String & query_id,
    ExchangeConnectionsPtr connections,
    const ExchangeStreamDestinations & exchange_stream_destinations,
    UInt16 streaming_exchange_port)
{
    return std::make_shared<StreamingExchangeLookup>(query_id, connections, exchange_stream_destinations, streaming_exchange_port);
}

}
