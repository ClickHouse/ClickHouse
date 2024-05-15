#pragma once

#include "ConnectionParameters.h"

#include <Client/Connection.h>
#include <Client/IServerConnection.h>
#include <Client/LocalConnection.h>
#include <Client/LineReader.h>
#include <IO/ConnectionTimeouts.h>
#include <atomic>
#include <thread>


namespace DB
{

class Suggest : public LineReader::Suggest, boost::noncopyable
{
public:
    Suggest() = default;

    ~Suggest()
    {
        if (loading_thread.joinable())
            loading_thread.join();
    }

    /// Load suggestions for clickhouse-client.
    template <typename ConnectionType>
    void load(ContextPtr context, const ConnectionParameters & connection_parameters, Int32 suggestion_limit, bool wait_for_load);

    void load(IServerConnection & connection,
              const ConnectionTimeouts & timeouts,
              Int32 suggestion_limit,
              const ClientInfo & client_info);

    /// Older server versions cannot execute the query loading suggestions.
    static constexpr int MIN_SERVER_REVISION = DBMS_MIN_PROTOCOL_VERSION_WITH_VIEW_IF_PERMITTED;

    int getLastError() const { return last_error.load(); }

private:
    void fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query, const ClientInfo & client_info);

    void fillWordsFromBlock(const Block & block);

    /// Words are fetched asynchronously.
    std::thread loading_thread;

    std::atomic<int> last_error { -1 };
};

}
