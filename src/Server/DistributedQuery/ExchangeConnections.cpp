#ifdef OS_LINUX
#include <mutex>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/FutureConnection.h>

namespace DB
{

void ExchangeConnections::addConnection(const String & query_id, const String & exchange_stream_id, Poco::Net::StreamSocket socket)
{
    LOG_TRACE(log, "Adding connection for query id {} exchange stream {}", query_id, exchange_stream_id);

    const auto connection_key = std::make_pair(query_id, exchange_stream_id);

    std::lock_guard lock(mutex);

    /// Check if a FutureConnection already exists (getConnection was called first)
    if (auto it = pending_connections.find(connection_key); it != pending_connections.end())
    {
        /// getConnection was called first, set the socket on the existing FutureConnection
        it->second->setSocket(socket);
        pending_connections.erase(it);

    }
    else
    {
        /// getConnection hasn't been called yet, create a new FutureConnection
        /// and keep it in the map until getConnection is called
        auto & element = pending_connections[connection_key];
        chassert(!element);
        element = std::make_shared<FutureConnection>();
        element->setSocket(socket);
    }
}

FutureConnectionPtr ExchangeConnections::getConnection(const String & query_id, const String & exchange_stream_id)
{
    LOG_TRACE(log, "Getting connection for query id {} exchange stream {}", query_id, exchange_stream_id);

    const auto connection_key = std::make_pair(query_id, exchange_stream_id);

    std::lock_guard lock(mutex);

    if (auto it = pending_connections.find(connection_key); it != pending_connections.end())
    {
        /// addConnection was called first, get the existing FutureConnection
        auto result = it->second;
        pending_connections.erase(it);
        return result;
    }
    else
    {
        /// addConnection hasn't been called yet, create a new FutureConnection
        /// It will be populated by addConnection later
        LOG_WARNING(log, "getConnection: key ({}, {}) not found in map. Map has {} entries: [{}]",
            query_id, exchange_stream_id,
            pending_connections.size(),
            [&]()
            {
                String s;
                for (const auto & [k, _] : pending_connections)
                    s += fmt::format("({},{})", k.first, k.second);
                return s;
            }());
        auto & element = pending_connections[connection_key];
        chassert(!element);
        element = std::make_shared<FutureConnection>();
        return element;
    }
}

}
#endif
