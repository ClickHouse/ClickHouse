#pragma once

#include "config.h"

#if USE_PULSAR

#include <Storages/Pulsar/Client/Client.h>
#include <Storages/Pulsar/Client/Protocol.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/Logger.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <chrono>
#include <mutex>


/// Internal parts of the client: the connections and the lookups. There are no threads: everything runs inside the
/// calls of the consumer or producer that owns the connections, and only `Interrupter` is used from other threads.
namespace DB::Pulsar
{

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

/// Wakes up the calls in progress from another thread by shutting down their sockets.
class Interrupter
{
public:
    void interrupt() noexcept;
    bool isInterrupted() const;

    /// The socket is shut down right away if the interrupter was already triggered.
    void add(int fd);
    void remove(int fd);

private:
    mutable std::mutex mutex;
    bool interrupted = false;
    VectorWithMemoryTracking<int> fds;
};

struct ClientState
{
    ServiceUrl service;
    ClientConfiguration configuration;
    Limits limits;
    Interrupter interrupter;
    /// The service host to try first for the next service connection.
    std::atomic<size_t> next_service_host = 0;
    std::atomic<UInt64> next_serial = 1;
    LoggerPtr log;
};

using ClientStatePtr = std::shared_ptr<ClientState>;

/// Where a connection goes.
struct BrokerAddress
{
    /// The hosts to connect to, tried in order: the service hosts, or one broker.
    VectorWithMemoryTracking<std::pair<String, UInt16>> hosts;
    /// The logical broker when connecting through a proxy (the service URL).
    String proxy_to_broker_url;

    bool operator==(const BrokerAddress &) const = default;
    [[nodiscard]] String toString() const;
};

class Connection;

class FrameHandler
{
public:
    /// `buffer` owns the frame, `frame.tail` points into it.
    virtual void onFrame(Connection & connection, Frame & frame, const SharedBuffer & buffer) = 0;

protected:
    ~FrameHandler() = default;
};

/// One TCP connection to a broker, with non-blocking I/O: it never blocks the calling thread.
class Connection
{
public:
    enum class State : uint8_t
    {
        TCPConnecting,
        TlsHandshake,
        ConnectSent,
        Ready,
        /// Lost or closed by the broker; not an error by itself.
        Closed,
    };

    /// Starts connecting. `owner_interrupter` may be null.
    Connection(ClientState & client_, BrokerAddress address_, Interrupter * owner_interrupter_);
    ~Connection();

    Connection(const Connection &) = delete;
    Connection & operator=(const Connection &) = delete;

    State state() const { return current_state; }
    bool isReady() const { return current_state == State::Ready; }
    bool isClosed() const { return current_state == State::Closed; }
    const BrokerAddress & address() const { return broker_address; }
    UInt64 id() const { return serial; }
    size_t maxMessageSize() const { return limits.max_message_size; }

    /// Queues a frame; sent by the next `process`. Dropped if the connection is closed.
    void send(const PulsarProto::BaseCommand & command);
    void sendFrame(StringWithMemoryTracking bytes);
    bool hasPendingOutput() const { return !output.empty(); }

    /// Makes progress without blocking: completes the connection, sends the queued output, reads about
    /// 1 MiB and passes the received frames to `handler` (apart from `CONNECTED`, `PING` and `PONG`).
    /// Throws on a failure to connect and on a protocol violation. A lost connection becomes `Closed`.
    void process(FrameHandler & handler);

    /// The descriptor and the `poll` events to wait for, -1 when closed.
    int fd() const;
    Int16 pollEvents() const;
    /// Whether decrypted data is buffered in the TLS session, so that `poll` would not see it.
    bool hasBufferedInput() const;
    /// When `process` must be called even without socket events (time limits and keepalive).
    TimePoint wakeUpTime() const;

    /// Tries to send the queued output once without blocking, and closes the socket.
    void close() noexcept;

private:
    void startNextAddress();
    void onTCPConnected();
    void sendConnect();
    void verifyPeerHostName();
    void processOutput();
    void processInput(FrameHandler & handler);
    void handleFrame(FrameHandler & handler, std::string_view body);
    void markClosed(const String & reason);
    void unregisterSocket();
    void checkTimeouts();

    ClientState & client;
    const BrokerAddress broker_address;
    Interrupter * owner_interrupter;
    const UInt64 serial;
    Limits limits;
    LoggerPtr log;

    State current_state = State::TCPConnecting;
    TimePoint step_started;
    size_t host_index = 0;
    VectorWithMemoryTracking<Poco::Net::SocketAddress> addresses;
    size_t address_index = 0;
    String last_error;
    /// The host the socket connects to.
    String host;
    bool tls_wants_write = false;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    int registered_fd = -1;

    StringWithMemoryTracking input;
    size_t input_begin = 0;
    DequeWithMemoryTracking<StringWithMemoryTracking> output;
    size_t output_offset = 0;
    TimePoint last_write_progress;
    TimePoint last_received;
    std::optional<TimePoint> ping_sent;

    StringWithMemoryTracking read_buffer;
    Frame frame;
};

/// The connections of one consumer or producer, and the lookups made on them. Used by one thread at a time.
class Session final : private FrameHandler
{
public:
    class Owner : public FrameHandler
    {
    public:
        /// The connection is about to be destroyed (lost or closed by the broker).
        virtual void onConnectionClosed(Connection & connection) = 0;

    protected:
        ~Owner() = default;
    };

    /// A resumable request on the service. Either `error` or the result is set when it completes.
    struct Request
    {
        String topic;
        bool done = false;
        std::optional<String> error;
        /// `PARTITIONED_METADATA`
        UInt32 partitions = 0;
        /// `LOOKUP`
        BrokerAddress broker;

        bool is_lookup = false;
        bool authoritative = false;
        size_t redirects = 0;
        /// The connection the request was sent on, and its id there; 0 when not sent yet.
        UInt64 connection_id = 0;
        UInt64 request_id = 0;
        BrokerAddress target;
        TimePoint started;
    };
    using RequestPtr = std::shared_ptr<Request>;

    Session(ClientStatePtr client_, Owner & owner_, Interrupter * owner_interrupter_);
    ~Session();

    ClientState & client() { return *client_state; }

    RequestPtr lookup(const String & topic);
    RequestPtr partitionedMetadata(const String & topic);

    /// Returns the connection to `address`, creating it if needed.
    Connection & connection(const BrokerAddress & address);
    /// Unique within the session.
    UInt64 nextRequestId();
    /// Whether `nextRequestId` returned this id. A response to such a request that is not awaited anymore is ignored:
    /// the broker may answer after closing the consumer or producer, or after the request expired.
    [[nodiscard]] bool wasIssued(UInt64 request_id) const;
    /// Null if the connection does not exist anymore.
    Connection * findConnection(UInt64 id);

    /// Advances the requests and all connections without blocking. Throws on failures, see `Connection::process`.
    void process();
    /// Waits for socket events on all connections until `deadline` or until one of them needs `process`.
    void wait(TimePoint deadline);

    [[nodiscard]] bool hasPendingOutput() const;
    /// Throws `ABORTED` if the client was closed or the owner was interrupted.
    void checkInterrupted() const;

    void close() noexcept;

private:
    void onFrame(Connection & connection, Frame & frame, const SharedBuffer & buffer) override;
    void sendRequests();
    void sendRequest(Request & request);
    void completeLookup(const RequestPtr & request, const PulsarProto::CommandLookupTopicResponse & response);
    Connection & serviceConnection();
    [[nodiscard]] BrokerAddress parseBrokerUrl(const String & url) const;

    ClientStatePtr client_state;
    Owner & owner;
    Interrupter * owner_interrupter;
    /// Small, in the order of creation.
    VectorWithMemoryTracking<std::unique_ptr<Connection>> connections;
    UnorderedMapWithMemoryTracking<UInt64, RequestPtr> requests_in_flight;
    VectorWithMemoryTracking<RequestPtr> requests_to_send;
    UInt64 service_connection_id = 0;
    UInt64 next_request_id = 1;
    LoggerPtr log;
};

/// As in the Pulsar C++ client.
constexpr size_t MAX_LOOKUP_REDIRECTS = 20;

[[noreturn]] void throwUnexpected(const char * what);

/// Within the operation timeout.
std::unique_ptr<Producer> createProducer(ClientStatePtr client, String topic, const ProducerConfiguration & configuration);

}

#endif
