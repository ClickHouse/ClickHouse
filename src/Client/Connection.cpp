#include <memory>
#include <Poco/Net/NetException.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <IO/TimeoutSetter.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Client/Connection.h>
#include <Client/ConnectionParameters.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/CurrentMetrics.h>
#include <Common/DNSResolver.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/randomSeed.h>
#include "Core/Block.h"
#include <Interpreters/ClientInfo.h>
#include <Compression/CompressionFactory.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISink.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <pcg_random.hpp>
#include <base/scope_guard.h>

#include <Common/config_version.h>
#include <Common/config.h>

#if USE_SSL
#    include <Poco/Net/SecureStreamSocket.h>
#endif

namespace CurrentMetrics
{
    extern const Metric SendScalars;
    extern const Metric SendExternalTables;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int EMPTY_DATA_PASSED;
}

Connection::~Connection() = default;

Connection::Connection(const String & host_, UInt16 port_,
    const String & default_database_,
    const String & user_, const String & password_,
    const String & quota_key_,
    const String & cluster_,
    const String & cluster_secret_,
    const String & client_name_,
    Protocol::Compression compression_,
    Protocol::Secure secure_,
    Poco::Timespan sync_request_timeout_)
    : host(host_), port(port_), default_database(default_database_)
    , user(user_), password(password_), quota_key(quota_key_)
    , cluster(cluster_)
    , cluster_secret(cluster_secret_)
    , client_name(client_name_)
    , compression(compression_)
    , secure(secure_)
    , sync_request_timeout(sync_request_timeout_)
    , log_wrapper(*this)
{
    /// Don't connect immediately, only on first need.

    if (user.empty())
        user = "default";

    setDescription();
}


void Connection::connect(const ConnectionTimeouts & timeouts)
{
    try
    {
        LOG_TRACE(log_wrapper.get(), "Connecting. Database: {}. User: {}{}{}",
            default_database.empty() ? "(not specified)" : default_database,
            user,
            static_cast<bool>(secure) ? ". Secure" : "",
            static_cast<bool>(compression) ? "" : ". Uncompressed");

        auto addresses = DNSResolver::instance().resolveAddressList(host, port);
        const auto & connection_timeout = static_cast<bool>(secure) ? timeouts.secure_connection_timeout : timeouts.connection_timeout;

        for (auto it = addresses.begin(); it != addresses.end();)
        {
            if (connected)
                disconnect();

            if (static_cast<bool>(secure))
            {
#if USE_SSL
                socket = std::make_unique<Poco::Net::SecureStreamSocket>();

                /// we resolve the ip when we open SecureStreamSocket, so to make Server Name Indication (SNI)
                /// work we need to pass host name separately. It will be send into TLS Hello packet to let
                /// the server know which host we want to talk with (single IP can process requests for multiple hosts using SNI).
                static_cast<Poco::Net::SecureStreamSocket*>(socket.get())->setPeerHostName(host);
#else
                throw Exception{"tcp_secure protocol is disabled because poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
            }
            else
            {
                socket = std::make_unique<Poco::Net::StreamSocket>();
            }

            try
            {
                socket->connect(*it, connection_timeout);
                current_resolved_address = *it;
                break;
            }
            catch (Poco::Net::NetException &)
            {
                if (++it == addresses.end())
                    throw;
                continue;
            }
            catch (Poco::TimeoutException &)
            {
                if (++it == addresses.end())
                    throw;
                continue;
            }
        }

        socket->setReceiveTimeout(timeouts.receive_timeout);
        socket->setSendTimeout(timeouts.send_timeout);
        socket->setNoDelay(true);
        if (timeouts.tcp_keep_alive_timeout.totalSeconds())
        {
            socket->setKeepAlive(true);
            socket->setOption(IPPROTO_TCP,
#if defined(TCP_KEEPALIVE)
                TCP_KEEPALIVE
#else
                TCP_KEEPIDLE  // __APPLE__
#endif
                , timeouts.tcp_keep_alive_timeout);
        }

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        in->setAsyncCallback(std::move(async_callback));

        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;

        sendHello();
        receiveHello();
        if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM)
            sendAddendum();

        LOG_TRACE(log_wrapper.get(), "Connected to {} server version {}.{}.{}.",
            server_name, server_version_major, server_version_minor, server_version_patch);
    }
    catch (Poco::Net::NetException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(e.displayText() + " (" + getDescription() + ")", ErrorCodes::NETWORK_ERROR);
    }
    catch (Poco::TimeoutException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        /// This exception can only be thrown from socket->connect(), so add information about connection timeout.
        const auto & connection_timeout = static_cast<bool>(secure) ? timeouts.secure_connection_timeout : timeouts.connection_timeout;
        throw NetException(
            ErrorCodes::SOCKET_TIMEOUT,
            "{} ({}, connection timeout {} ms)",
            e.displayText(),
            getDescription(),
            connection_timeout.totalMilliseconds());
    }
}


void Connection::disconnect()
{
    maybe_compressed_out = nullptr;
    in = nullptr;
    last_input_packet_type.reset();
    out = nullptr; // can write to socket
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}


void Connection::sendHello()
{
    /** Disallow control characters in user controlled parameters
      *  to mitigate the possibility of SSRF.
      * The user may do server side requests with 'remote' table function.
      * Malicious user with full r/w access to ClickHouse
      *  may use 'remote' table function to forge requests
      *  to another services in the network other than ClickHouse (examples: SMTP).
      * Limiting number of possible characters in user-controlled part of handshake
      *  will mitigate this possibility but doesn't solve it completely.
      */
    auto has_control_character = [](const std::string & s)
    {
        for (auto c : s)
            if (isControlASCII(c))
                return true;
        return false;
    };

    if (has_control_character(default_database)
        || has_control_character(user)
        || has_control_character(password))
        throw Exception("Parameters 'default_database', 'user' and 'password' must not contain ASCII control characters", ErrorCodes::BAD_ARGUMENTS);

    writeVarUInt(Protocol::Client::Hello, *out);
    writeStringBinary((DBMS_NAME " ") + client_name, *out);
    writeVarUInt(DBMS_VERSION_MAJOR, *out);
    writeVarUInt(DBMS_VERSION_MINOR, *out);
    // NOTE For backward compatibility of the protocol, client cannot send its version_patch.
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, *out);
    writeStringBinary(default_database, *out);
    /// If interserver-secret is used, one do not need password
    /// (NOTE we do not check for DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET, since we cannot ignore inter-server secret if it was requested)
    if (!cluster_secret.empty())
    {
        writeStringBinary(USER_INTERSERVER_MARKER, *out);
        writeStringBinary("" /* password */, *out);

#if USE_SSL
        sendClusterNameAndSalt();
#else
        throw Exception(
            "Inter-server secret support is disabled, because ClickHouse was built without SSL library",
            ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
    else
    {
        writeStringBinary(user, *out);
        writeStringBinary(password, *out);
    }

    out->next();
}


void Connection::sendAddendum()
{
    if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY)
        writeStringBinary(quota_key, *out);
    out->next();
}


void Connection::receiveHello()
{
    /// Receive hello packet.
    UInt64 packet_type = 0;

    /// Prevent read after eof in readVarUInt in case of reset connection
    /// (Poco should throw such exception while reading from socket but
    /// sometimes it doesn't for unknown reason)
    if (in->eof())
        throw Poco::Net::NetException("Connection reset by peer");

    readVarUInt(packet_type, *in);
    if (packet_type == Protocol::Server::Hello)
    {
        readStringBinary(server_name, *in);
        readVarUInt(server_version_major, *in);
        readVarUInt(server_version_minor, *in);
        readVarUInt(server_revision, *in);
        if (server_revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
            readStringBinary(server_timezone, *in);
        if (server_revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
            readStringBinary(server_display_name, *in);
        if (server_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
            readVarUInt(server_version_patch, *in);
        else
            server_version_patch = server_revision;
    }
    else if (packet_type == Protocol::Server::Exception)
        receiveException()->rethrow();
    else
    {
        /// Close connection, to not stay in unsynchronised state.
        disconnect();
        throwUnexpectedPacket(packet_type, "Hello or Exception");
    }
}

void Connection::setDefaultDatabase(const String & database)
{
    default_database = database;
}

const String & Connection::getDefaultDatabase() const
{
    return default_database;
}

const String & Connection::getDescription() const
{
    return description;
}

const String & Connection::getHost() const
{
    return host;
}

UInt16 Connection::getPort() const
{
    return port;
}

void Connection::getServerVersion(const ConnectionTimeouts & timeouts,
                                  String & name,
                                  UInt64 & version_major,
                                  UInt64 & version_minor,
                                  UInt64 & version_patch,
                                  UInt64 & revision)
{
    if (!connected)
        connect(timeouts);

    name = server_name;
    version_major = server_version_major;
    version_minor = server_version_minor;
    version_patch = server_version_patch;
    revision = server_revision;
}

UInt64 Connection::getServerRevision(const ConnectionTimeouts & timeouts)
{
    if (!connected)
        connect(timeouts);

    return server_revision;
}

const String & Connection::getServerTimezone(const ConnectionTimeouts & timeouts)
{
    if (!connected)
        connect(timeouts);

    return server_timezone;
}

const String & Connection::getServerDisplayName(const ConnectionTimeouts & timeouts)
{
    if (!connected)
        connect(timeouts);

    return server_display_name;
}

void Connection::forceConnected(const ConnectionTimeouts & timeouts)
{
    if (!connected)
    {
        connect(timeouts);
    }
    else if (!ping())
    {
        LOG_TRACE(log_wrapper.get(), "Connection was closed, will reconnect.");
        connect(timeouts);
    }
}

#if USE_SSL
void Connection::sendClusterNameAndSalt()
{
    pcg64_fast rng(randomSeed());
    UInt64 rand = rng();

    salt = encodeSHA256(&rand, sizeof(rand));

    writeStringBinary(cluster, *out);
    writeStringBinary(salt, *out);
}
#endif

bool Connection::ping()
{
    try
    {
        TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);

        UInt64 pong = 0;
        writeVarUInt(Protocol::Client::Ping, *out);
        out->next();

        if (in->eof())
            return false;

        readVarUInt(pong, *in);

        /// Could receive late packets with progress. TODO: Maybe possible to fix.
        while (pong == Protocol::Server::Progress)
        {
            receiveProgress();

            if (in->eof())
                return false;

            readVarUInt(pong, *in);
        }

        if (pong != Protocol::Server::Pong)
            throwUnexpectedPacket(pong, "Pong");
    }
    catch (const Poco::Exception & e)
    {
        /// Explicitly disconnect since ping() can receive EndOfStream,
        /// and in this case this ping() will return false,
        /// while next ping() may return true.
        disconnect();
        LOG_TRACE(log_wrapper.get(), fmt::runtime(e.displayText()));
        return false;
    }

    return true;
}

TablesStatusResponse Connection::getTablesStatus(const ConnectionTimeouts & timeouts,
                                                 const TablesStatusRequest & request)
{
    if (!connected)
        connect(timeouts);

    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);

    writeVarUInt(Protocol::Client::TablesStatusRequest, *out);
    request.write(*out, server_revision);
    out->next();

    UInt64 response_type = 0;
    readVarUInt(response_type, *in);

    if (response_type == Protocol::Server::Exception)
        receiveException()->rethrow();
    else if (response_type != Protocol::Server::TablesStatusResponse)
        throwUnexpectedPacket(response_type, "TablesStatusResponse");

    TablesStatusResponse response;
    response.read(*in, server_revision);
    return response;
}


void Connection::sendQuery(
    const ConnectionTimeouts & timeouts,
    const String & query,
    const String & query_id_,
    UInt64 stage,
    const Settings * settings,
    const ClientInfo * client_info,
    bool with_pending_data,
    std::function<void(const Progress &)>)
{
    if (!connected)
        connect(timeouts);

    /// Query is not executed within sendQuery() function.
    ///
    /// And what this means that temporary timeout (via TimeoutSetter) is not
    /// enough, since next query can use timeout from the previous query in this case.
    socket->setReceiveTimeout(timeouts.receive_timeout);
    socket->setSendTimeout(timeouts.send_timeout);

    if (settings)
    {
        std::optional<int> level;
        std::string method = Poco::toUpper(settings->network_compression_method.toString());

        /// Bad custom logic
        if (method == "ZSTD")
            level = settings->network_zstd_compression_level;

        CompressionCodecFactory::instance().validateCodec(method, level, !settings->allow_suspicious_codecs, settings->allow_experimental_codecs);
        compression_codec = CompressionCodecFactory::instance().get(method, level);
    }
    else
        compression_codec = CompressionCodecFactory::instance().getDefaultCodec();

    query_id = query_id_;

    writeVarUInt(Protocol::Client::Query, *out);
    writeStringBinary(query_id, *out);

    /// Client info.
    if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
    {
        if (client_info && !client_info->empty())
            client_info->write(*out, server_revision);
        else
            ClientInfo().write(*out, server_revision);
    }

    /// Per query settings.
    if (settings)
    {
        auto settings_format = (server_revision >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS) ? SettingsWriteFormat::STRINGS_WITH_FLAGS
                                                                                                          : SettingsWriteFormat::BINARY;
        settings->write(*out, settings_format);
    }
    else
        writeStringBinary("" /* empty string is a marker of the end of settings */, *out);

    /// Interserver secret
    if (server_revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET)
    {
        /// Hash
        ///
        /// Send correct hash only for !INITIAL_QUERY, due to:
        /// - this will avoid extra protocol complexity for simplest cases
        /// - there is no need in hash for the INITIAL_QUERY anyway
        ///   (since there is no secure/unsecure changes)
        if (client_info && !cluster_secret.empty() && client_info->query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        {
#if USE_SSL
            std::string data(salt);
            data += cluster_secret;
            data += query;
            data += query_id;
            data += client_info->initial_user;
            /// TODO: add source/target host/ip-address

            std::string hash = encodeSHA256(data);
            writeStringBinary(hash, *out);
#else
        throw Exception(
            "Inter-server secret support is disabled, because ClickHouse was built without SSL library",
            ErrorCodes::SUPPORT_IS_DISABLED);
#endif
        }
        else
            writeStringBinary("", *out);
    }

    writeVarUInt(stage, *out);
    writeVarUInt(static_cast<bool>(compression), *out);

    writeStringBinary(query, *out);

    maybe_compressed_in.reset();
    maybe_compressed_out.reset();
    block_in.reset();
    block_logs_in.reset();
    block_profile_events_in.reset();
    block_out.reset();

    /// Send empty block which means end of data.
    if (!with_pending_data)
    {
        sendData(Block(), "", false);
        out->next();
    }
}


void Connection::sendCancel()
{
    /// If we already disconnected.
    if (!out)
        return;

    writeVarUInt(Protocol::Client::Cancel, *out);
    out->next();
}


void Connection::sendData(const Block & block, const String & name, bool scalar)
{
    if (!block_out)
    {
        if (compression == Protocol::Compression::Enable)
            maybe_compressed_out = std::make_unique<CompressedWriteBuffer>(*out, compression_codec);
        else
            maybe_compressed_out = out;

        block_out = std::make_unique<NativeWriter>(*maybe_compressed_out, server_revision, block.cloneEmpty());
    }

    if (scalar)
        writeVarUInt(Protocol::Client::Scalar, *out);
    else
        writeVarUInt(Protocol::Client::Data, *out);
    writeStringBinary(name, *out);

    size_t prev_bytes = out->count();

    block_out->write(block);
    maybe_compressed_out->next();
    out->next();

    if (throttler)
        throttler->add(out->count() - prev_bytes);
}

void Connection::sendIgnoredPartUUIDs(const std::vector<UUID> & uuids)
{
    writeVarUInt(Protocol::Client::IgnoredPartUUIDs, *out);
    writeVectorBinary(uuids, *out);
    out->next();
}


void Connection::sendReadTaskResponse(const String & response)
{
    writeVarUInt(Protocol::Client::ReadTaskResponse, *out);
    writeVarUInt(DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION, *out);
    writeStringBinary(response, *out);
    out->next();
}


void Connection::sendMergeTreeReadTaskResponse(const PartitionReadResponse & response)
{
    writeVarUInt(Protocol::Client::MergeTreeReadTaskResponse, *out);
    response.serialize(*out);
    out->next();
}

void Connection::sendPreparedData(ReadBuffer & input, size_t size, const String & name)
{
    /// NOTE 'Throttler' is not used in this method (could use, but it's not important right now).

    if (input.eof())
        throw Exception("Buffer is empty (some kind of corruption)", ErrorCodes::EMPTY_DATA_PASSED);

    writeVarUInt(Protocol::Client::Data, *out);
    writeStringBinary(name, *out);

    if (0 == size)
        copyData(input, *out);
    else
        copyData(input, *out, size);
    out->next();
}


void Connection::sendScalarsData(Scalars & data)
{
    /// Avoid sending scalars to old servers. Note that this isn't a full fix. We didn't introduce a
    /// dedicated revision after introducing scalars, so this will still break some versions with
    /// revision 54428.
    if (server_revision < DBMS_MIN_REVISION_WITH_SCALARS)
        return;

    if (data.empty())
        return;

    Stopwatch watch;
    size_t out_bytes = out ? out->count() : 0;
    size_t maybe_compressed_out_bytes = maybe_compressed_out ? maybe_compressed_out->count() : 0;
    size_t rows = 0;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::SendScalars};

    for (auto & elem : data)
    {
        rows += elem.second.rows();
        sendData(elem.second, elem.first, true /* scalar */);
    }

    out_bytes = out->count() - out_bytes;
    maybe_compressed_out_bytes = maybe_compressed_out->count() - maybe_compressed_out_bytes;
    double elapsed = watch.elapsedSeconds();

    if (compression == Protocol::Compression::Enable)
        LOG_DEBUG(log_wrapper.get(),
            "Sent data for {} scalars, total {} rows in {} sec., {} rows/sec., {} ({}/sec.), compressed {} times to {} ({}/sec.)",
            data.size(), rows, elapsed,
            static_cast<size_t>(rows / watch.elapsedSeconds()),
            ReadableSize(maybe_compressed_out_bytes),
            ReadableSize(maybe_compressed_out_bytes / watch.elapsedSeconds()),
            static_cast<double>(maybe_compressed_out_bytes) / out_bytes,
            ReadableSize(out_bytes),
            ReadableSize(out_bytes / watch.elapsedSeconds()));
    else
        LOG_DEBUG(log_wrapper.get(),
            "Sent data for {} scalars, total {} rows in {} sec., {} rows/sec., {} ({}/sec.), no compression.",
            data.size(), rows, elapsed,
            static_cast<size_t>(rows / watch.elapsedSeconds()),
            ReadableSize(maybe_compressed_out_bytes),
            ReadableSize(maybe_compressed_out_bytes / watch.elapsedSeconds()));
}

namespace
{
/// Sink which sends data for external table.
class ExternalTableDataSink : public ISink
{
public:
    using OnCancell = std::function<void()>;

    ExternalTableDataSink(Block header, Connection & connection_, ExternalTableData & table_data_, OnCancell callback)
            : ISink(std::move(header)), connection(connection_), table_data(table_data_),
              on_cancell(std::move(callback))
    {}

    String getName() const override { return "ExternalTableSink"; }

    size_t getNumReadRows() const { return num_rows; }

protected:
    void consume(Chunk chunk) override
    {
        if (table_data.is_cancelled)
        {
            on_cancell();
            return;
        }

        num_rows += chunk.getNumRows();

        auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());
        connection.sendData(block, table_data.table_name, false);
    }

private:
    Connection & connection;
    ExternalTableData & table_data;
    OnCancell on_cancell;
    size_t num_rows = 0;
};
}

void Connection::sendExternalTablesData(ExternalTablesData & data)
{
    if (data.empty())
    {
        /// Send empty block, which means end of data transfer.
        sendData(Block(), "", false);
        return;
    }

    Stopwatch watch;
    size_t out_bytes = out ? out->count() : 0;
    size_t maybe_compressed_out_bytes = maybe_compressed_out ? maybe_compressed_out->count() : 0;
    size_t rows = 0;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::SendExternalTables};

    for (auto & elem : data)
    {
        PipelineExecutorPtr executor;
        auto on_cancel = [& executor]() { executor->cancel(); };

        if (!elem->pipe)
            elem->pipe = elem->creating_pipe_callback();

        QueryPipelineBuilder pipeline = std::move(*elem->pipe);
        elem->pipe.reset();
        pipeline.resize(1);
        auto sink = std::make_shared<ExternalTableDataSink>(pipeline.getHeader(), *this, *elem, std::move(on_cancel));
        pipeline.setSinks([&](const Block &, QueryPipelineBuilder::StreamType type) -> ProcessorPtr
        {
            if (type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;
            return sink;
        });
        executor = pipeline.execute();
        executor->execute(/*num_threads = */ 1);

        auto read_rows = sink->getNumReadRows();
        rows += read_rows;

        /// If table is empty, send empty block with name.
        if (read_rows == 0)
            sendData(sink->getPort().getHeader(), elem->table_name, false);
    }

    /// Send empty block, which means end of data transfer.
    sendData(Block(), "", false);

    out_bytes = out->count() - out_bytes;
    maybe_compressed_out_bytes = maybe_compressed_out->count() - maybe_compressed_out_bytes;
    double elapsed = watch.elapsedSeconds();

    if (compression == Protocol::Compression::Enable)
        LOG_DEBUG(log_wrapper.get(),
            "Sent data for {} external tables, total {} rows in {} sec., {} rows/sec., {} ({}/sec.), compressed {} times to {} ({}/sec.)",
            data.size(), rows, elapsed,
            static_cast<size_t>(rows / watch.elapsedSeconds()),
            ReadableSize(maybe_compressed_out_bytes),
            ReadableSize(maybe_compressed_out_bytes / watch.elapsedSeconds()),
            static_cast<double>(maybe_compressed_out_bytes) / out_bytes,
            ReadableSize(out_bytes),
            ReadableSize(out_bytes / watch.elapsedSeconds()));
    else
        LOG_DEBUG(log_wrapper.get(),
            "Sent data for {} external tables, total {} rows in {} sec., {} rows/sec., {} ({}/sec.), no compression.",
            data.size(), rows, elapsed,
            static_cast<size_t>(rows / watch.elapsedSeconds()),
            ReadableSize(maybe_compressed_out_bytes),
            ReadableSize(maybe_compressed_out_bytes / watch.elapsedSeconds()));
}

std::optional<Poco::Net::SocketAddress> Connection::getResolvedAddress() const
{
    return current_resolved_address;
}


bool Connection::poll(size_t timeout_microseconds)
{
    return static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_microseconds);
}


bool Connection::hasReadPendingData() const
{
    return last_input_packet_type.has_value() || static_cast<const ReadBufferFromPocoSocket &>(*in).hasPendingData();
}


std::optional<UInt64> Connection::checkPacket(size_t timeout_microseconds)
{
    if (last_input_packet_type.has_value())
        return last_input_packet_type;

    if (hasReadPendingData() || poll(timeout_microseconds))
    {
        UInt64 packet_type;
        readVarUInt(packet_type, *in);

        last_input_packet_type.emplace(packet_type);
        return last_input_packet_type;
    }

    return {};
}


Packet Connection::receivePacket()
{
    try
    {
        Packet res;

        /// Have we already read packet type?
        if (last_input_packet_type)
        {
            res.type = *last_input_packet_type;
            last_input_packet_type.reset();
        }
        else
        {
            readVarUInt(res.type, *in);
        }

        switch (res.type)
        {
            case Protocol::Server::Data:
            case Protocol::Server::Totals:
            case Protocol::Server::Extremes:
                res.block = receiveData();
                return res;

            case Protocol::Server::Exception:
                res.exception = receiveException();
                return res;

            case Protocol::Server::Progress:
                res.progress = receiveProgress();
                return res;

            case Protocol::Server::ProfileInfo:
                res.profile_info = receiveProfileInfo();
                return res;

            case Protocol::Server::Log:
                res.block = receiveLogData();
                return res;

            case Protocol::Server::TableColumns:
                res.multistring_message = receiveMultistringMessage(res.type);
                return res;

            case Protocol::Server::EndOfStream:
                return res;

            case Protocol::Server::PartUUIDs:
                readVectorBinary(res.part_uuids, *in);
                return res;

            case Protocol::Server::ReadTaskRequest:
                return res;

            case Protocol::Server::MergeTreeReadTaskRequest:
                res.request = receivePartitionReadRequest();
                return res;

            case Protocol::Server::ProfileEvents:
                res.block = receiveProfileEvents();
                return res;

            default:
                /// In unknown state, disconnect - to not leave unsynchronised connection.
                disconnect();
                throw Exception("Unknown packet "
                    + toString(res.type)
                    + " from server " + getDescription(), ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
        }
    }
    catch (Exception & e)
    {
        /// This is to consider ATTEMPT_TO_READ_AFTER_EOF as a remote exception.
        e.setRemoteException();

        /// Add server address to exception message, if need.
        if (e.code() != ErrorCodes::UNKNOWN_PACKET_FROM_SERVER)
            e.addMessage("while receiving packet from " + getDescription());

        throw;
    }
}


Block Connection::receiveData()
{
    initBlockInput();
    return receiveDataImpl(*block_in);
}


Block Connection::receiveLogData()
{
    initBlockLogsInput();
    return receiveDataImpl(*block_logs_in);
}


Block Connection::receiveDataImpl(NativeReader & reader)
{
    String external_table_name;
    readStringBinary(external_table_name, *in);

    size_t prev_bytes = in->count();

    /// Read one block from network.
    Block res = reader.read();

    if (throttler)
        throttler->add(in->count() - prev_bytes);

    return res;
}


Block Connection::receiveProfileEvents()
{
    initBlockProfileEventsInput();
    return receiveDataImpl(*block_profile_events_in);
}


void Connection::initInputBuffers()
{

}


void Connection::initBlockInput()
{
    if (!block_in)
    {
        if (!maybe_compressed_in)
        {
            if (compression == Protocol::Compression::Enable)
                maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in);
            else
                maybe_compressed_in = in;
        }

        block_in = std::make_unique<NativeReader>(*maybe_compressed_in, server_revision);
    }
}


void Connection::initBlockLogsInput()
{
    if (!block_logs_in)
    {
        /// Have to return superset of SystemLogsQueue::getSampleBlock() columns
        block_logs_in = std::make_unique<NativeReader>(*in, server_revision);
    }
}


void Connection::initBlockProfileEventsInput()
{
    if (!block_profile_events_in)
    {
        block_profile_events_in = std::make_unique<NativeReader>(*in, server_revision);
    }
}


void Connection::setDescription()
{
    auto resolved_address = getResolvedAddress();
    description = host + ":" + toString(port);

    if (resolved_address)
    {
        auto ip_address = resolved_address->host().toString();
        if (host != ip_address)
            description += ", " + ip_address;
    }
}


std::unique_ptr<Exception> Connection::receiveException() const
{
    return std::make_unique<Exception>(readException(*in, "Received from " + getDescription(), true /* remote */));
}


std::vector<String> Connection::receiveMultistringMessage(UInt64 msg_type) const
{
    size_t num = Protocol::Server::stringsInMessage(msg_type);
    std::vector<String> strings(num);
    for (size_t i = 0; i < num; ++i)
        readStringBinary(strings[i], *in);
    return strings;
}


Progress Connection::receiveProgress() const
{
    Progress progress;
    progress.read(*in, server_revision);
    return progress;
}


ProfileInfo Connection::receiveProfileInfo() const
{
    ProfileInfo profile_info;
    profile_info.read(*in);
    return profile_info;
}

PartitionReadRequest Connection::receivePartitionReadRequest() const
{
    PartitionReadRequest request;
    request.deserialize(*in);
    return request;
}


void Connection::throwUnexpectedPacket(UInt64 packet_type, const char * expected) const
{
    throw NetException(
            "Unexpected packet from server " + getDescription() + " (expected " + expected
            + ", got " + String(Protocol::Server::toString(packet_type)) + ")",
            ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}

ServerConnectionPtr Connection::createConnection(const ConnectionParameters & parameters, ContextPtr)
{
    return std::make_unique<Connection>(
        parameters.host,
        parameters.port,
        parameters.default_database,
        parameters.user,
        parameters.password,
        parameters.quota_key,
        "", /* cluster */
        "", /* cluster_secret */
        "client",
        parameters.compression,
        parameters.security);
}

}
