#include <mutex>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/FutureConnection.h>

namespace DB
{

void ExchangeConnections::addConnection(const String & query_id, const String & exchange_stream_id, Poco::Net::StreamSocket socket)
{
    LOG_TRACE(log, "Adding connection for query id {} exchange stream {}", query_id, exchange_stream_id);

    bool should_remove = false;
    {
        std::lock_guard lock(mutex);

        /// Check if a FutureConnection already exists (getConnection was called first)
        auto & element = connections[query_id][exchange_stream_id];
        if (!element)
        {
            /// getConnection hasn't been called yet, create a new FutureConnection
            /// and keep it in the map until getConnection is called
            element = std::make_shared<FutureConnection>();
        }

        /// Set the socket on the future connection
        element->setSocket(socket);

        /// If getConnection was already called, we can remove the entry now
        should_remove = element->wasRetrieved();
    }

    if (should_remove)
    {
        std::lock_guard lock(mutex);
        connections[query_id].erase(exchange_stream_id);
        if (connections[query_id].empty())
            connections.erase(query_id);
    }
}

FutureConnectionPtr ExchangeConnections::getConnection(const String & query_id, const String & exchange_stream_id)
{
    LOG_TRACE(log, "Getting connection for query id {} exchange stream {}", query_id, exchange_stream_id);

    FutureConnectionPtr result;
    bool should_remove = false;
    {
        std::lock_guard lock(mutex);

        /// Check if a FutureConnection already exists (addConnection was called first)
        auto & element = connections[query_id][exchange_stream_id];
        if (!element)
        {
            /// addConnection hasn't been called yet, create a new FutureConnection
            /// It will be populated by addConnection later
            element = std::make_shared<FutureConnection>();
        }

        /// Mark as retrieved
        element->markRetrieved();

        /// Get the FutureConnection to return
        result = element;

        /// If socket was already set (addConnection was called first), we can remove the entry now
        should_remove = element->isReady();
    }

    if (should_remove)
    {
        std::lock_guard lock(mutex);
        connections[query_id].erase(exchange_stream_id);
        if (connections[query_id].empty())
            connections.erase(query_id);
    }

    return result;
}

}
