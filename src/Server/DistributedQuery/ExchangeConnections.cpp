#ifdef OS_LINUX
#include <mutex>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/FutureConnection.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

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
        auto & element = pending_connections[connection_key];
        chassert(!element);
        element = std::make_shared<FutureConnection>();
        return element;
    }
}

void ExchangeConnections::cleanupQuery(const String & query_id)
{
    std::vector<FutureConnectionPtr> to_cancel;
    {
        std::lock_guard lock(mutex);
        for (auto it = pending_connections.begin(); it != pending_connections.end();)
        {
            if (it->first.first == query_id)
            {
                to_cancel.push_back(it->second);
                it = pending_connections.erase(it);
            }
            else
                ++it;
        }
    }

    if (to_cancel.empty())
        return;

    LOG_TRACE(log, "Cleaning up {} pending exchange connections for query id {}", to_cancel.size(), query_id);

    auto exception = std::make_exception_ptr(
        Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Exchange connection cancelled, query id {}", query_id));
    for (auto & future : to_cancel)
    {
        /// `cancel` may throw if the promise was already satisfied by a concurrent
        /// addConnection/getConnection pairing; swallow so other waiters still get woken.
        try
        {
            future->cancel(exception);
        }
        catch (...)
        {
            tryLogCurrentException(log, "FutureConnection cancel");
        }
    }
}

}
#endif
