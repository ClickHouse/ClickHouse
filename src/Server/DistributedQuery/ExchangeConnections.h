#pragma once

#include <Client/Connection.h>
#include <base/defines.h>
#include <Poco/Net/StreamSocket.h>
#include <Server/DistributedQuery/FutureConnection.h>

#include <boost/container_hash/hash.hpp>

#include <future>
#include <memory>
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

private:
    std::mutex mutex;
    using ConnectionKey = std::pair<String, String>; /// query_id, exchange_stream_id
    using ConnectionsMap = std::unordered_map<ConnectionKey, FutureConnectionPtr, boost::hash<ConnectionKey>>;
    ConnectionsMap pending_connections;
    LoggerPtr log = getLogger("ExchangeConnections");
};

using ExchangeConnectionsPtr = std::shared_ptr<ExchangeConnections>;

}
