#pragma once

#include <Client/ConnectionParameters.h>

#include <Client/Connection.h>
#include <Client/IServerConnection.h>
#include <Client/LocalConnection.h>
#include <Client/LineReader.h>
#include <Core/ProtocolDefines.h>
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

    /// Load suggestions through an already established connection, synchronously.
    /// Errors are reported to `error_stream` (inside the embedded client, `std::cerr`
    /// belongs to the server process, not to the user's terminal).
    void load(IServerConnection & connection,
              const ConnectionTimeouts & timeouts,
              Int32 suggestion_limit,
              const ClientInfo & client_info,
              const Settings & settings,
              std::ostream & error_stream);

    /// Older server versions cannot execute the query loading suggestions.
    static constexpr int MIN_SERVER_REVISION = DBMS_MIN_PROTOCOL_VERSION_WITH_VIEW_IF_PERMITTED;

    int getLastError() const { return last_error.load(); }

    /// Whether the last suggestions exchange ended with the connection protocol in sync: with
    /// `EndOfStream` or with a server exception, which is the terminal packet of the exchange
    /// (the query is sent with `with_pending_data` = false, and the server preserves the
    /// connection after an ordinary exception). A transport or client-side failure in the middle
    /// of the exchange leaves this false. Only meaningful for `load(IServerConnection &, ...)`,
    /// which shares the connection with the regular queries of the session.
    bool lastExchangeEndedInSync() const { return last_exchange_ended_in_sync.load(); }

private:
    void fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query, const ClientInfo & client_info, const Settings & settings);

    void fillWordsFromBlock(const Block & block);

    /// Words are fetched asynchronously.
    std::thread loading_thread;

    std::atomic<int> last_error { -1 };

    std::atomic<bool> last_exchange_ended_in_sync { false };
};

}
