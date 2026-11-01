#pragma once

#include <Poco/Net/StreamSocket.h>

#include <functional>
#include <memory>

#include <Common/callOnce.h>
#include <Common/SSHWrapper.h>
#include <Common/SettingsChanges.h>
#include <Client/IServerConnection.h>
#include <Core/Defines.h>

#include <Formats/FormatSettings.h>

#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <IO/WriteBufferFromPocoSocketChunked.h>

#include <Interpreters/TablesStatus.h>
#include <Interpreters/Context_fwd.h>

#include <Compression/ICompressionCodec.h>

#include <Storages/MergeTree/RequestResponse.h>

#include <optional>

#include "config.h"

namespace DB
{

struct Settings;
struct TimeoutSetter;

class JWTProvider;
class Connection;
struct ConnectionParameters;
struct ClusterFunctionReadTaskResponse;

using ConnectionPtr = std::shared_ptr<Connection>;
using Connections = std::vector<ConnectionPtr>;

class NativeReader;
class NativeWriter;

/** Connection with database server, to use by client.
  * How to use - see Core/Protocol.h
  * (Implementation of server end - see Server/TCPHandler.h)
  *
  * As 'default_database' empty string could be passed
  *  - in that case, server will use it's own default database.
  */
class Connection : public IServerConnection
{
    friend class MultiplexedConnections;

public:
    using SocketFactory = std::function<std::unique_ptr<Poco::Net::StreamSocket>(bool secure)>;

    static std::unique_ptr<Poco::Net::StreamSocket> defaultSocketFactory(bool secure);

    Connection(const String & host_, UInt16 port_,
        const String & default_database_,
        const String & user_, const String & password_,
        const String & proto_send_chunked_, const String & proto_recv_chunked_,
        const SSHKey & ssh_private_key_,
        const String & jwt_,
        const String & quota_key_,
        const String & cluster_,
        const String & cluster_secret_,
        const String & client_name_,
        Protocol::Compression compression_,
        Protocol::Secure secure_,
        const String & tls_sni_override_,
        const String & bind_host_
#if USE_JWT_CPP && USE_SSL
        , std::shared_ptr<JWTProvider> jwt_provider_ = nullptr
#endif
        , SocketFactory socket_factory_ = &Connection::defaultSocketFactory
    );

    ~Connection() override;

    IServerConnection::Type getConnectionType() const override { return IServerConnection::Type::SERVER; }

    static ServerConnectionPtr createConnection(const ConnectionParameters & parameters, ContextPtr context);

    /// Tell the connection which address of the host is already known to accept connections, so that it is
    /// tried first. The host can resolve to several addresses and connecting to them is sequential, so an
    /// unresponsive address in front of the list delays the connection by a whole connection timeout.
    /// The address is only a preference: if it is gone by the time of the connection (or of a reconnect),
    /// the remaining addresses are tried as usual.
    void setPreferredAddress(const Poco::Net::SocketAddress & address) { preferred_address = address; }

    /// The address of the host the connection has been established to, if it has been resolved.
    /// It is the address to pass to `setPreferredAddress` of a subsequent connection to the same host.
    std::optional<Poco::Net::SocketAddress> getResolvedAddress() const;

    /// Hand over an already-established TCP connection to `address`, to be used instead of opening a new
    /// one. The socket has to be connected and non-blocking; the handshake (including the TLS handshake
    /// for a secure connection) is still performed by this class. It is used only for the first connect,
    /// so a later reconnect opens a connection of its own.
    void setAdoptedSocket(const Poco::Net::SocketAddress & address, const Poco::Net::StreamSocket & connected_socket)
    {
        adopted_address = address;
        adopted_socket = connected_socket;
    }

    /// Set throttler of network traffic. One throttler could be used for multiple connections to limit total traffic.
    void setThrottler(const ThrottlerPtr & throttler_) override
    {
        throttler = throttler_;
    }

    /// Change default database. Changes will take effect on next reconnect.
    void setDefaultDatabase(const String & database) override;

    void getServerVersion(const ConnectionTimeouts & timeouts,
                          String & name,
                          UInt64 & version_major,
                          UInt64 & version_minor,
                          UInt64 & version_patch,
                          UInt64 & revision) override;

    UInt64 getServerRevision(const ConnectionTimeouts & timeouts) override;

    const String & getServerTimezone(const ConnectionTimeouts & timeouts) override;
    const String & getServerDisplayName(const ConnectionTimeouts & timeouts) override;

    const SettingsChanges & settingsFromServer() const;

    /// For log and exception messages.
    const String & getDescription(bool with_extra = false) const override; /// NOLINT
    const String & getHost() const;
    UInt16 getPort() const;
    const String & getDefaultDatabase() const;

    Protocol::Compression getCompression() const { return compression; }

    std::vector<std::pair<String, String>> getPasswordComplexityRules() const override { return password_complexity_rules; }

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const NameToNameMap& query_parameters,
        const String & query_id_/* = "" */,
        UInt64 stage/* = QueryProcessingStage::Complete */,
        const Settings * settings/* = nullptr */,
        const ClientInfo * client_info/* = nullptr */,
        bool with_pending_data/* = false */,
        const std::vector<String> & external_roles,
        std::function<void(const Progress &)> process_progress_callback) override;

    void sendQueryPlan(const QueryPlan & query_plan) override;

    void sendCancel() override;

    void sendData(const Block & block, const String & name/* = "" */, bool scalar/* = false */) override;

    void sendMergeTreeReadTaskResponse(const ParallelReadResponse & response) override;

    void sendMergeTreeAllRangesAnnouncementResponse(const InitialAllRangesAnnouncementResponse & response) override;

    void sendExternalTablesData(ExternalTablesData & data) override;

    bool poll(size_t timeout_microseconds/* = 0 */) override;

    bool hasReadPendingData() const override;

    std::optional<UInt64> checkPacket(size_t timeout_microseconds/* = 0*/) override;

    Packet receivePacket() override;
    UInt64 receivePacketType() override;

    void forceConnected(const ConnectionTimeouts & timeouts) override;

    bool isConnected() const override { return connected && in && out && !in->isCanceled() && !out->isCanceled(); }

    bool checkConnected(const ConnectionTimeouts & timeouts) override { return isConnected() && ping(timeouts); }

    /// Note that a server that went away without closing the connection is not detected here, and
    /// neither is a close that has not arrived yet; that only shows up when the connection is used.
    /// Pinging the server to find out would add a round trip to every query, and a pong that does
    /// not arrive in time is indistinguishable from a closed connection, so it would make the client
    /// drop live sessions under load.
    bool checkConnectedWithoutRoundTrip() override { return isConnected() && !isStale(); }

    void disconnect() override;

    /// Send prepared block of data (serialized and, if need, compressed), that will be read from 'input'.
    /// You could pass size of serialized/compressed block.
    void sendPreparedData(ReadBuffer & input, size_t size, const String & name = "");

    void sendClusterFunctionReadTaskResponse(const ClusterFunctionReadTaskResponse & response);
    /// Send all scalars.
    void sendScalarsData(Scalars & data) override;

    TablesStatusResponse getTablesStatus(const ConnectionTimeouts & timeouts,
                                         const TablesStatusRequest & request);

    size_t outBytesCount() const { return out ? out->count() : 0; }
    size_t inBytesCount() const { return in ? in->count() : 0; }

    Poco::Net::Socket * getSocket() { return socket.get(); }

    /// Each time read from socket blocks and async_callback is set, it will be called. You can poll socket inside it.
    void setAsyncCallback(AsyncCallback async_callback_)
    {
        async_callback = std::move(async_callback_);
        if (in)
            in->setAsyncCallback(async_callback);
        if (out)
            out->setAsyncCallback(async_callback);
    }

    bool haveMoreAddressesToConnect() const { return have_more_addresses_to_connect; }

    void setAddressConnectTimeoutExpired() { address_connect_timeout_expired = true; }

    void setFormatSettings(const FormatSettings & settings) override
    {
        format_settings = settings;
    }

    UInt64 getParallelReplicasProtocolVersion() const { return server_parallel_replicas_protocol_version; }
    UInt64 getQueryPlanSerializationVersion() const { return server_query_plan_serialization_version; }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;
    String proto_send_chunked;
    String proto_recv_chunked;
    String proto_send_chunked_srv;
    String proto_recv_chunked_srv;
#if USE_SSH
    SSHKey ssh_private_key;
#endif
    String quota_key;
#if USE_JWT_CPP && USE_SSL
    String jwt;
    std::shared_ptr<JWTProvider> jwt_provider;
#endif

    /// For inter-server authorization
    String cluster;
    String cluster_secret;
    /// For DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET
    String salt;
    /// For DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2
    std::optional<UInt64> nonce;

    /// Address is resolved during the first connection (or the following reconnects)
    /// Use it only for logging purposes
    std::optional<Poco::Net::SocketAddress> current_resolved_address;

    /// See setPreferredAddress.
    std::optional<Poco::Net::SocketAddress> preferred_address;

    /// See setAdoptedSocket. Consumed by the first connect.
    std::optional<Poco::Net::SocketAddress> adopted_address;
    std::optional<Poco::Net::StreamSocket> adopted_socket;

    /// For messages in log and in exceptions.
    String description;
    String full_description;
    void setDescription();

    String client_name;

    bool connected = false;

    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_version_patch = 0;
    UInt64 server_revision = 0;
    UInt64 server_parallel_replicas_protocol_version = 0;
    UInt64 worker_cluster_function_protocol_version = 0;
    UInt64 server_query_plan_serialization_version = 0;
    String server_timezone;
    String server_display_name;
    SettingsChanges settings_from_server;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBufferFromPocoSocketChunked> in;
    std::shared_ptr<WriteBufferFromPocoSocketChunked> out;
    std::optional<UInt64> last_input_packet_type;

    String query_id;
    Protocol::Compression compression;        /// Enable data compression for communication.
    Protocol::Secure secure;             /// Enable data encryption for communication.
    String tls_sni_override;             /// Override for TLS SNI field.
    String bind_host;
    SocketFactory socket_factory;

    /// What compression settings to use while sending data for INSERT queries and external tables.
    CompressionCodecPtr compression_codec;

    /** If not nullptr, used to limit network traffic.
      * Only traffic for transferring blocks is accounted. Other packets don't.
      */
    ThrottlerPtr throttler;

    std::vector<std::pair<String, String>> password_complexity_rules;

    /// From where to read query execution result.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    std::unique_ptr<NativeReader> block_in;
    std::unique_ptr<NativeReader> block_logs_in;
    std::unique_ptr<NativeReader> block_profile_events_in;

    /// Where to write data for INSERT.
    std::shared_ptr<WriteBuffer> maybe_compressed_out;
    std::unique_ptr<NativeWriter> block_out;

    /// True if there are more resolved addresses to try when connecting (hostname may resolve to multiple IPs).
    bool have_more_addresses_to_connect = false;
    /// Set by async callback when the per-address connect timeout expires, used to abort the current attempt.
    bool address_connect_timeout_expired = false;

    /// Logger is created lazily, for avoid to run DNS request in constructor.
    class LoggerWrapper
    {
    public:
        explicit LoggerWrapper(Connection & parent_)
            : log(nullptr), parent(parent_)
        {
        }

        LoggerPtr get()
        {
            callOnce(log_initialized, [&] {
                log = getLogger("Connection (" + parent.getDescription() + ")");
            });

            return log;
        }

    private:
        OnceFlag log_initialized;
        LoggerPtr log;
        Connection & parent;
    };

    LoggerWrapper log_wrapper;

    AsyncCallback async_callback = {};

    std::optional<FormatSettings> format_settings;

    void connect(const ConnectionTimeouts & timeouts);

    /// Establishes the transport for `connect`: either by connecting to one of the addresses the host
    /// resolves to, or by taking over a connection that has already been established (see setAdoptedSocket).
    void connectToAnyAddress(const ConnectionTimeouts & timeouts);
    void adoptSocket(Poco::Net::StreamSocket connected_socket);
    void sendHello();

    void cancel() noexcept;
    void reset() noexcept;

#if USE_SSH
    void performHandshakeForSSHAuth();
#endif

    void sendAddendum();
    void receiveHello();

#if USE_SSL
    void sendClusterNameAndSalt();
#endif
    bool ping(const ConnectionTimeouts & timeouts);

    /// Whether the connection can no longer serve a request, checked without a round trip.
    bool isStale();

    Block receiveData();
    Block receiveLogData();
    Block receiveDataImpl(NativeReader & reader);
    Block receiveProfileEvents();

    String receiveTableColumns();
    std::unique_ptr<Exception> receiveException() const;
    Progress receiveProgress() const;
    ParallelReadRequest receiveParallelReadRequest() const;
    InitialAllRangesAnnouncement receiveInitialParallelReadAnnouncement() const;
    ProfileInfo receiveProfileInfo() const;

    void initInputBuffers();
    void initMaybeCompressedInput();
    void initBlockInput();
    void initBlockLogsInput();
    void initBlockProfileEventsInput();

    void ensureConnected() const;

    [[noreturn]] void throwUnexpectedPacket(UInt64 packet_type, const char * expected, TimeoutSetter * timeout_setter = nullptr);
};

template <typename Conn>
class AsyncCallbackSetter
{
public:
    AsyncCallbackSetter(Conn * connection_, AsyncCallback async_callback) : connection(connection_)
    {
        connection->setAsyncCallback(std::move(async_callback));
    }

    ~AsyncCallbackSetter()
    {
        connection->setAsyncCallback({});
    }
private:
    Conn * connection;
};

}
