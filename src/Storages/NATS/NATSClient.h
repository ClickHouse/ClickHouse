#pragma once

#include <base/types.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/Logger.h>
#include <Common/SecretString.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <chrono>
#include <memory>
#include <optional>
#include <vector>


namespace DB
{

class ReadBufferFromPocoSocket;
class WriteBufferFromPocoSocket;

/// A minimal synchronous client of the NATS protocol, enough for the `NATS` table engine:
/// https://docs.nats.io/reference/reference-protocols/nats-protocol
///
/// Every call works on the socket directly: there is no event loop and no background thread. Messages of the
/// subscriptions, `PING` of the server, and replies are read only inside the calls of the connection, and messages are
/// buffered if they arrive while waiting for something else. A connection that is not used for a while must be
/// serviced with `processIncoming`, otherwise the server closes it after unanswered `PING`s.
/// JetStream is used through its API subjects on top of this, see `NATSJetStreamConsumer` and `NATSJetStreamProducer`.
namespace NATSClient
{

/// `[nats://|tls://][user[:password]@]host[:port]`. The user info ends at the last '@', as in the official clients.
/// A user without a password is a token.
struct ServerAddress
{
    String host;
    UInt16 port = 4222;
    String user;
    SecretString password;
    SecretString token;
    bool tls = false;

    static ServerAddress parse(std::string_view url);
};

struct Authentication
{
    String user;
    SecretString password;
    SecretString token;
    /// The content of a NATS credentials file: a user JWT and the NKey seed that signs the nonce of the server.
    SecretString credentials;
};

struct TLSSettings
{
    /// Use TLS even if the server does not require it.
    bool secure = false;
    bool verify_server = true;
    String ca_file;
    String client_cert_file;
    String client_key_file;
};

/// Checks that the files of the TLS settings can be loaded.
void validateTLSSettings(const TLSSettings & tls);

struct ConnectionSettings
{
    std::vector<ServerAddress> servers;
    Authentication authentication;
    TLSSettings tls;
    Poco::Timespan connect_timeout{5, 0};
    /// Bounds waiting for a reply of the server and reading of everything that has started arriving.
    Poco::Timespan request_timeout{10, 0};
    /// The client sends `PING` this often while it is used, and gives up after this many unanswered ones.
    std::chrono::milliseconds ping_interval{60'000};
    size_t max_pings_out = 3;
};

struct Message
{
    UInt64 sid = 0;
    String subject;
    String reply;
    String data;
    /// The status of a message with headers, e.g. 408 for an expired JetStream pull request, 0 for a plain message.
    UInt16 status = 0;
    String status_description;
};

class Connection
{
public:
    /// Connects to the first server that accepts the connection. Throws `CANNOT_CONNECT_NATS`.
    Connection(const ConnectionSettings & settings, LoggerPtr log);
    ~Connection();

    /// Sends what is buffered and closes the socket. Does not throw.
    void close();

    /// Whether nothing has failed and the server has not closed the connection. Reads what is already there.
    [[nodiscard]] bool isUsable();

    /// Why the connection is not usable, or the last error that the server reported.
    [[nodiscard]] const String & getError() const { return error; }

    [[nodiscard]] UInt64 getMaxPayload() const { return max_payload; }

    UInt64 subscribe(const String & subject, const String & queue_group);
    void unsubscribe(UInt64 sid);

    /// Buffers the message. It is sent by `flush`, or once the buffer is full.
    void publish(const String & subject, const String & reply, std::string_view data);

    /// Sends what is buffered and waits until the server has processed it (a `PING` and its `PONG`).
    void flush();

    /// Waits for the next message of a subscription at most `timeout`.
    std::optional<Message> receive(Poco::Timespan timeout);

    /// Publishes with a reply subject and waits for the reply at most `timeout`. Throws `TIMEOUT_EXCEEDED`.
    Message request(const String & subject, std::string_view data, Poco::Timespan timeout);

    /// A subject unique for this connection, to receive replies on.
    String newInbox();

    /// Reads what is already there, answers `PING` of the server, and sends a `PING` when it is due.
    void processIncoming();

private:
    using Clock = std::chrono::steady_clock;

    void connectToServer(const ServerAddress & server);
    void handshake(const ServerAddress & server);
    void startTLS(const ServerAddress & server);

    void writeLine(std::string_view line);
    String readLine(Clock::time_point deadline);
    void readExact(char * to, size_t size, Clock::time_point deadline);

    /// Reads one operation of the server, waiting for its start at most until `start_deadline`.
    /// Returns false if nothing arrived. Messages are buffered, `PING` is answered.
    bool readOperation(Clock::time_point start_deadline);
    void sendPingIfNeeded();

    void checkUsable() const;
    [[noreturn]] void fail(const String & message);

    template <typename F>
    auto guarded(F && f);

    const ConnectionSettings settings;
    LoggerPtr log;
    String server_for_log;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::unique_ptr<ReadBufferFromPocoSocket> in;
    std::unique_ptr<WriteBufferFromPocoSocket> out;

    UInt64 max_payload = 1024 * 1024;
    String nonce;
    bool server_requires_tls = false;
    bool server_has_headers = false;

    UInt64 next_sid = 1;
    String inbox_prefix;
    UInt64 next_inbox = 0;
    /// The subscription to `<inbox_prefix>.*` for the replies of `request`, created on its first use.
    UInt64 replies_sid = 0;

    UInt64 pings_sent = 0;
    UInt64 pongs_received = 0;
    Clock::time_point last_ping;

    String error;

    DequeWithMemoryTracking<Message> messages;
    DequeWithMemoryTracking<Message> replies;
};

}

}
