#pragma once

#ifdef OS_LINUX
#include <Client/Connection.h>
#include <base/defines.h>
#include <Poco/Net/StreamSocket.h>
#include <Server/DistributedQuery/FutureConnection.h>

#include <boost/container_hash/hash.hpp>

#include <deque>
#include <future>
#include <memory>
#include <unordered_set>
#include <utility>


namespace DB
{

/// Stores conncections initiated by remote tasks and allows local tasks to find them.
class ExchangeConnections : boost::noncopyable
{
public:
    ExchangeConnections() = default;
    virtual ~ExchangeConnections() = default;

    /// TODO: move to Context instead of this singleton
    static std::shared_ptr<ExchangeConnections> instance()
    {
        static std::shared_ptr<ExchangeConnections> self = std::make_shared<ExchangeConnections>();
        return self;
    }

    void addConnection(const String & query_id, const String & exchange_stream_id, Poco::Net::StreamSocket socket);

    /// Get a future connection that will be ready once the remote side connects.
    /// Returns immediately without blocking.
    FutureConnectionPtr getConnection(const String & query_id, const String & exchange_stream_id);

    /// Remove all pending entries that belong to the given query and wake any
    /// waiters on them with a cancellation exception. Called from the
    /// distributed-query cleanup path so connections that never paired up do
    /// not leak `FutureConnection`s/eventfds across the lifetime of the server.
    void cleanupQuery(const String & query_id);

    /// Drop the pending entries for the given streams of a query, waking any waiter with a
    /// cancellation. Used by a worker task to release only its own output streams on completion or
    /// cancellation, without disturbing sibling tasks of the same query that own other streams.
    void removePendingStreams(const String & query_id, const std::vector<String> & exchange_stream_ids);

private:
    std::mutex mutex;
    using ConnectionKey = std::pair<String, String>; /// query_id, exchange_stream_id
    /// One rendezvous per exchange stream: the producer provides the socket (`addConnection`) and the
    /// consumer takes the future (`getConnection`), in either order. The flags let a duplicate be
    /// rejected instead of dropping the socket or handing it to two readers. Released by
    /// `removePendingStreams` or `cleanupQuery`.
    struct ConnectionSlot
    {
        FutureConnectionPtr future = std::make_shared<FutureConnection>();
        bool socket_delivered = false;   /// a producer connection has provided its socket
        bool consumer_assigned = false;  /// a consumer has taken the future
    };
    using ConnectionsMap = std::unordered_map<ConnectionKey, ConnectionSlot, boost::hash<ConnectionKey>>;
    ConnectionsMap pending_connections;
    /// Queries that have been cleaned up. Late add/get for these query ids must not
    /// (re)create pending entries — otherwise the entry would have no owner and leak.
    /// Bounded: query ids are tracked in insertion order and the oldest tombstone is dropped past
    /// the cap, so the set cannot grow without limit on a long-running server.
    static constexpr size_t MAX_CANCELLED_QUERIES = 100000;
    std::unordered_set<String> cancelled_queries;
    std::deque<String> cancelled_queries_order;

    /// Streams already released by `removePendingStreams`. A worker never calls `cleanupQuery`, so
    /// without this a connection arriving after the owning task finished would recreate a slot that
    /// nothing ever releases, leaking an eventfd. Late add/get for these streams is rejected. Bounded
    /// with the same FIFO eviction as `cancelled_queries`.
    static constexpr size_t MAX_RELEASED_STREAMS = 100000;
    std::unordered_set<ConnectionKey, boost::hash<ConnectionKey>> released_streams;
    std::deque<ConnectionKey> released_streams_order;

    /// Records a stream as released, evicting the oldest entry past the cap. Call under `mutex`.
    void markStreamReleased(const ConnectionKey & key);

    LoggerPtr log = getLogger("ExchangeConnections");
};

using ExchangeConnectionsPtr = std::shared_ptr<ExchangeConnections>;

}

#endif
