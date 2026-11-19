#pragma once

#include <Server/DistributedQuery/ExchangeControlConnectionHandler.h>
#include <Interpreters/InterserverCredentials.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>
#include <base/defines.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_set>

namespace DB
{

class StatelessTaskExecutor;

namespace StatelessWorkerControlProtocol
{
struct ControlHello;
}

/// Serves the control connections of a stateless worker (`StatelessWorkerControlProtocol`), which the streaming-exchange
/// server hands over. Each accepted connection is a session of one query with its own thread, the only reader and writer
/// of the connection; a session that closes cancels and forgets the tasks it started that the initiator did not forget.
class StatelessWorkerControlServer final : public IExchangeControlConnectionHandler
{
public:
    struct Limits
    {
        /// `stateless_worker_server.max_queries`.
        size_t max_queries = 1000;
        /// `stateless_worker_server.max_control_sessions`.
        size_t max_sessions = 4000;
        /// A session closes after this long without a frame from the initiator, which pings every second.
        std::chrono::milliseconds silence_timeout{30'000};
        /// How long a closing session waits for the tasks it cancels.
        std::chrono::milliseconds reclaim_timeout{60'000};
    };

    using CredentialsGetter = std::function<InterserverCredentialsPtr()>;

    /// `get_credentials` returns the interserver credentials a hello must present, null for none.
    StatelessWorkerControlServer(Limits limits_, CredentialsGetter get_credentials_);
    ~StatelessWorkerControlServer() override;

    const std::shared_ptr<StatelessTaskExecutor> & getExecutor() const { return executor; }

    void handleControlConnection(
        Poco::Net::StreamSocket socket, const StreamingExchangeProtocol::PacketHeader & header, const ExchangeConnectionAuthenticator & authenticate) override;

    /// Refuses new connections, closes the sessions, shuts the executor down and waits for the sessions to end.
    void shutdown();

    /// Waits until no session runs; false at `deadline`. Exposed for tests.
    bool waitForSessions(std::chrono::steady_clock::time_point deadline);

private:
    class Session;
    using SessionPtr = std::shared_ptr<Session>;

    StatelessWorkerControlProtocol::ControlHello receiveHello(Poco::Net::StreamSocket & socket, const StreamingExchangeProtocol::PacketHeader & header) const;
    void authenticate(const StatelessWorkerControlProtocol::ControlHello & hello, const ExchangeConnectionAuthenticator & authenticate_token) const;
    /// Admits the hello's query and runs a session for the connection; throws, having taken nothing, when it cannot.
    void startSession(const Poco::Net::StreamSocket & socket, const StatelessWorkerControlProtocol::ControlHello & hello);
    void removeSession(const SessionPtr & session);

    const Limits limits;
    const CredentialsGetter get_credentials;
    const std::shared_ptr<StatelessTaskExecutor> executor;
    const LoggerPtr log;

    std::mutex sessions_mutex;
    std::condition_variable sessions_changed;
    std::unordered_set<SessionPtr> sessions TSA_GUARDED_BY(sessions_mutex);
    /// Written under `sessions_mutex`, read by the sessions without it.
    std::atomic<bool> shutdown_called{false};

    /// Last, so that it joins the sessions before the members they use are destroyed.
    ThreadPool session_pool;
};

}
