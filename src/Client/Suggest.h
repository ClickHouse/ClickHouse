#pragma once

#include "ConnectionParameters.h"

#include <Client/Connection.h>
#include <IO/ConnectionTimeouts.h>
#include <common/LineReader.h>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
}

class Suggest : public LineReader::Suggest, boost::noncopyable
{
public:
    Suggest();

    ~Suggest()
    {
        if (loading_thread.joinable())
            loading_thread.join();
    }

    /// Load suggestions for clickhouse-client.
    void load(const ConnectionParameters & connection_parameters, Int32 suggestion_limit);

    /// Load suggestions for clickhouse-local.
    void load(ContextPtr context);

    /// Older server versions cannot execute the query above.
    static constexpr int MIN_SERVER_REVISION = 54406;

private:
    void loadImpl(Connection & connection, const ConnectionTimeouts & timeouts, Int32 suggestion_limit);

    void fetch(Connection & connection, const ConnectionTimeouts & timeouts, const std::string & query);

    void fillWordsFromBlock(const Block & block);

    /// Words are fetched asynchronously.
    std::thread loading_thread;
};

}
