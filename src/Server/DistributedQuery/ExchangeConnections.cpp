#include <mutex>
#include <Server/DistributedQuery/ExchangeConnections.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

void ExchangeConnections::addConnection(const String & query_id, const String & exchange_stream_id, Poco::Net::StreamSocket socket)
{
    std::lock_guard lock(mutex);
    auto & element = connections[query_id][exchange_stream_id];
    element.promise.set_value(std::move(socket));
}

Poco::Net::StreamSocket ExchangeConnections::getConnection(const String & query_id, const String & exchange_stream_id)
{
    LOG_TRACE(log, "Getting connection for query id {} exchange stream {}", query_id, exchange_stream_id);

    std::shared_future<Poco::Net::StreamSocket> result;
    {
        std::lock_guard lock(mutex);
        auto & element = connections[query_id][exchange_stream_id];
        result = element.future;
    }

    /// Wait until the connection is established
    /// TODO: think how to replace this synchronous wait with returning "delayed" connection
    if (result.wait_for(std::chrono::seconds(60)) != std::future_status::ready)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout while waiting for connection to exchange stream {}", exchange_stream_id);

    /// Remove the entry from the map
    {
        std::lock_guard lock(mutex);
        connections[query_id].erase(exchange_stream_id);
        if (connections[query_id].empty())
            connections.erase(query_id);
    }

    return result.get();
}

}
