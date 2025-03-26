#pragma once

#include <Client/Connection.h>
#include <base/defines.h>
#include <Poco/Net/StreamSocket.h>

#include <future>
#include <memory>

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

    Poco::Net::StreamSocket getConnection(const String & query_id, const String & exchange_stream_id);

private:
    struct StreamConnection
    {
        std::promise<Poco::Net::StreamSocket> promise;
        std::shared_future<Poco::Net::StreamSocket> future = promise.get_future();
    };

    std::mutex mutex;
    std::unordered_map<String, std::unordered_map<String, StreamConnection>> connections;
    LoggerPtr log = getLogger("ExchangeConnections");
};

using ExchangeConnectionsPtr = std::shared_ptr<ExchangeConnections>;

}
