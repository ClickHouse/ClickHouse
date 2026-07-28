#if defined(OS_LINUX) || defined(OS_DARWIN)

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

    /// Refuse late arrivals for a cancelled query: getConnection cannot consume the
    /// entry anymore, so creating one would leak.
    if (cancelled_queries.contains(query_id))
    {
        LOG_TRACE(log, "Dropping late connection for cancelled query id {} exchange stream {}", query_id, exchange_stream_id);
        socket.close();
        return;
    }

    /// The owning task already released this stream; do not recreate a slot nothing will consume.
    if (released_streams.contains(connection_key))
    {
        LOG_TRACE(log, "Dropping connection for released query id {} exchange stream {}", query_id, exchange_stream_id);
        socket.close();
        return;
    }

    auto & slot = pending_connections[connection_key];

    /// One producer per stream. A duplicate (reconnect or repeated `SourceHello`) must not drop the
    /// first socket: close the new one and keep the original.
    if (slot.socket_delivered)
    {
        LOG_WARNING(log, "Dropping duplicate connection for query id {} exchange stream {}", query_id, exchange_stream_id);
        socket.close();
        return;
    }

    /// Deliver the socket: wakes a consumer that is already waiting, or stays ready until one takes it.
    slot.future->setSocket(socket);
    slot.socket_delivered = true;
}

FutureConnectionPtr ExchangeConnections::getConnection(const String & query_id, const String & exchange_stream_id)
{
    LOG_TRACE(log, "Getting connection for query id {} exchange stream {}", query_id, exchange_stream_id);

    const auto connection_key = std::make_pair(query_id, exchange_stream_id);

    std::lock_guard lock(mutex);

    /// Cancelled query: return a pre-cancelled future so the caller fails on the next
    /// wait instead of creating a pending entry that nothing will satisfy.
    if (cancelled_queries.contains(query_id))
    {
        auto future_connection = std::make_shared<FutureConnection>();
        future_connection->cancel(std::make_exception_ptr(
            Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Exchange connection cancelled, query id {}", query_id)));
        return future_connection;
    }

    /// The owning task already released this stream; reject instead of creating a slot nothing frees.
    if (released_streams.contains(connection_key))
    {
        auto cancelled = std::make_shared<FutureConnection>();
        cancelled->cancel(std::make_exception_ptr(
            Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Exchange stream already released, query id {} stream {}", query_id, exchange_stream_id)));
        return cancelled;
    }

    auto & slot = pending_connections[connection_key];

    /// One consumer per stream. A duplicate (e.g. a task started twice) must not get the same
    /// socket; reject it with a cancelled future.
    if (slot.consumer_assigned)
    {
        LOG_WARNING(log, "Refusing duplicate consumer for query id {} exchange stream {}", query_id, exchange_stream_id);
        auto cancelled = std::make_shared<FutureConnection>();
        cancelled->cancel(std::make_exception_ptr(
            Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Duplicate exchange consumer, query id {} stream {}", query_id, exchange_stream_id)));
        return cancelled;
    }

    slot.consumer_assigned = true;
    /// Ready already if the producer connected first; otherwise `addConnection` wakes the waiter.
    return slot.future;
}

void ExchangeConnections::cleanupQuery(const String & query_id)
{
    std::vector<FutureConnectionPtr> to_cancel;
    {
        std::lock_guard lock(mutex);
        /// Mark the query so any addConnection/getConnection arriving after this point
        /// short-circuits and doesn't recreate an orphan entry. Keep the tombstone set bounded by
        /// evicting the oldest ids; late connections are only possible for recently-cleaned queries.
        if (cancelled_queries.insert(query_id).second)
        {
            cancelled_queries_order.push_back(query_id);
            while (cancelled_queries_order.size() > MAX_CANCELLED_QUERIES)
            {
                cancelled_queries.erase(cancelled_queries_order.front());
                cancelled_queries_order.pop_front();
            }
        }
        for (auto it = pending_connections.begin(); it != pending_connections.end();)
        {
            if (it->first.first == query_id)
            {
                to_cancel.push_back(it->second.future);
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
    /// `cancel` is a no-op if the connection already paired (`FutureConnection` completes at most once).
    for (auto & future : to_cancel)
        future->cancel(exception);
}

void ExchangeConnections::markStreamReleased(const ConnectionKey & key)
{
    if (released_streams.insert(key).second)
    {
        released_streams_order.push_back(key);
        while (released_streams_order.size() > MAX_RELEASED_STREAMS)
        {
            released_streams.erase(released_streams_order.front());
            released_streams_order.pop_front();
        }
    }
}

void ExchangeConnections::removePendingStreams(const String & query_id, const std::vector<String> & exchange_stream_ids)
{
    std::vector<FutureConnectionPtr> to_cancel;
    {
        std::lock_guard lock(mutex);
        for (const auto & exchange_stream_id : exchange_stream_ids)
        {
            const auto key = std::make_pair(query_id, exchange_stream_id);
            if (auto it = pending_connections.find(key); it != pending_connections.end())
            {
                to_cancel.push_back(it->second.future);
                pending_connections.erase(it);
            }
            /// Tombstone the stream so a connection arriving after this task finished is rejected
            /// instead of recreating an orphan slot (a worker never runs cleanupQuery).
            markStreamReleased(key);
        }
    }

    if (to_cancel.empty())
        return;

    LOG_TRACE(log, "Cleaning up {} pending exchange connections for query id {}", to_cancel.size(), query_id);

    auto exception = std::make_exception_ptr(
        Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Exchange connection cancelled, query id {}", query_id));
    /// `cancel` is a no-op if the connection already paired (`FutureConnection` completes at most once).
    for (auto & future : to_cancel)
        future->cancel(exception);
}

}

#endif
