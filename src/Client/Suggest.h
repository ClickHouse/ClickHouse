#pragma once

#include "ConnectionParameters.h"

#include <Client/Connection.h>
#include <Client/IServerConnection.h>
#include <Client/LocalConnection.h>
#include <IO/ConnectionTimeouts.h>
#include <base/LineReader.h>
#include <thread>
#include <mutex>


namespace DB
{

class Suggest : public LineReader::Suggest, boost::noncopyable
{
public:
    explicit Suggest(UInt64 reload_interval_ms_);
    ~Suggest();

    /// Schedule suggestion update for clickhouse-client/clickhouse-local.
    template <typename ConnectionType>
    void schedule(ContextPtr context, const ConnectionParameters & connection_parameters, Int32 suggestion_limit);

    /// Older server versions cannot execute the query above.
    static constexpr int MIN_SERVER_REVISION = 54406;

private:
    void fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query);

    template <typename ConnectionType>
    void load(ContextPtr context, const ConnectionParameters & connection_parameters, Int32 suggestion_limit);

    void fillWordsFromBlock(const Block & block);

    UInt64 reload_interval_ms;
    /// Words are fetched asynchronously.
    std::thread loading_thread;

    /// Keywords may be not up to date with ClickHouse parser.
    Words default_words;
    Words current_words;

    std::condition_variable exit_cv;
    std::atomic_bool exited = false;
    std::mutex exit_mutex;
};

}
