#include <iomanip>

#include <Poco/Net/NetException.h>
#include <Core/Defines.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Client/Connection.h>
#include <Client/TimeoutSetter.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/CurrentMetrics.h>
#include <Common/DNSResolver.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/config_version.h>
#include <Interpreters/ClientInfo.h>

#include <Common/config.h>
#if USE_POCO_NETSSL
#include <Poco/Net/SecureStreamSocket.h>
#endif


namespace CurrentMetrics
{
    extern const Metric SendExternalTables;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int SERVER_REVISION_IS_TOO_OLD;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
}


void Connection::connect()
{
    try
    {
        if (connected)
            disconnect();

        LOG_TRACE(log_wrapper.get(), "Connecting. Database: " << (default_database.empty() ? "(not specified)" : default_database) << ". User: " << user
        << (static_cast<bool>(secure) ? ". Secure" : "") << (static_cast<bool>(compression) ? "" : ". Uncompressed") );

        if (static_cast<bool>(secure))
        {
#if USE_POCO_NETSSL
            socket = std::make_unique<Poco::Net::SecureStreamSocket>();
#else
            throw Exception{"tcp_secure protocol is disabled because poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        }
        else
        {
            socket = std::make_unique<Poco::Net::StreamSocket>();
        }

        current_resolved_address = DNSResolver::instance().resolveAddress(host, port);

        socket->connect(current_resolved_address, timeouts.connection_timeout);
        socket->setReceiveTimeout(timeouts.receive_timeout);
        socket->setSendTimeout(timeouts.send_timeout);
        socket->setNoDelay(true);

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;

        sendHello();
        receiveHello();

        LOG_TRACE(log_wrapper.get(), "Connected to " << server_name
            << " server version " << server_version_major
            << "." << server_version_minor
            << "." << server_version_patch
            << ".");
    }
    catch (Poco::Net::NetException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(e.displayText(), "(" + getDescription() + ")", ErrorCodes::NETWORK_ERROR);
    }
    catch (Poco::TimeoutException & e)
    {
        disconnect();

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(e.displayText(), "(" + getDescription() + ")", ErrorCodes::SOCKET_TIMEOUT);
    }
}


void Connection::disconnect()
{
    //LOG_TRACE(log_wrapper.get(), "Disconnecting");

    in = nullptr;
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
    writeVarUInt(ClickHouseRevision::get(), *out);
    writeStringBinary(default_database, *out);
    writeStringBinary(user, *out);
    writeStringBinary(password, *out);

    out->next();
}


void Connection::receiveHello()
{
    //LOG_TRACE(log_wrapper.get(), "Receiving hello");

    /// Receive hello packet.
    UInt64 packet_type = 0;

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

void Connection::getServerVersion(String & name, UInt64 & version_major, UInt64 & version_minor, UInt64 & version_patch, UInt64 & revision)
{
    if (!connected)
        connect();

    name = server_name;
    version_major = server_version_major;
    version_minor = server_version_minor;
    version_patch = server_version_patch;
    revision = server_revision;
}

const String & Connection::getServerTimezone()
{
    if (!connected)
        connect();

    return server_timezone;
}

const String & Connection::getServerDisplayName()
{
    if (!connected)
        connect();

    return server_display_name;
}

void Connection::forceConnected()
{
    if (!connected)
    {
        connect();
    }
    else if (!ping())
    {
        LOG_TRACE(log_wrapper.get(), "Connection was closed, will reconnect.");
        connect();
    }
}

bool Connection::ping()
{
    // LOG_TRACE(log_wrapper.get(), "Ping");

    TimeoutSetter timeout_setter(*socket, sync_request_timeout, true);
    try
    {
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
        LOG_TRACE(log_wrapper.get(), e.displayText());
        return false;
    }

    return true;
}

TablesStatusResponse Connection::getTablesStatus(const TablesStatusRequest & request)
{
    if (!connected)
        connect();

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
    const String & query,
    const String & query_id_,
    UInt64 stage,
    const Settings * settings,
    const ClientInfo * client_info,
    bool with_pending_data)
{
    if (!connected)
        connect();

    compression_settings = settings ? CompressionSettings(*settings) : CompressionSettings(CompressionMethod::LZ4);

    query_id = query_id_;

    //LOG_TRACE(log_wrapper.get(), "Sending query");

    writeVarUInt(Protocol::Client::Query, *out);
    writeStringBinary(query_id, *out);

    /// Client info.
    if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
    {
        ClientInfo client_info_to_send;

        if (!client_info)
        {
            /// No client info passed - means this query initiated by me.
            client_info_to_send.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
            client_info_to_send.fillOSUserHostNameAndVersionInfo();
            client_info_to_send.client_name = (DBMS_NAME " ") + client_name;
        }
        else
        {
            /// This query is initiated by another query.
            client_info_to_send = *client_info;
            client_info_to_send.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        }

        client_info_to_send.write(*out, server_revision);
    }

    /// Per query settings.
    if (settings)
        settings->serialize(*out);
    else
        writeStringBinary("", *out);

    writeVarUInt(stage, *out);
    writeVarUInt(static_cast<bool>(compression), *out);

    writeStringBinary(query, *out);

    maybe_compressed_in.reset();
    maybe_compressed_out.reset();
    block_in.reset();
    block_out.reset();

    /// Send empty block which means end of data.
    if (!with_pending_data)
    {
        sendData(Block());
        out->next();
    }
}


void Connection::sendCancel()
{
    //LOG_TRACE(log_wrapper.get(), "Sending cancel");

    writeVarUInt(Protocol::Client::Cancel, *out);
    out->next();
}


void Connection::sendData(const Block & block, const String & name)
{
    //LOG_TRACE(log_wrapper.get(), "Sending data");

    if (!block_out)
    {
        if (compression == Protocol::Compression::Enable)
            maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(*out, compression_settings);
        else
            maybe_compressed_out = out;

        block_out = std::make_shared<NativeBlockOutputStream>(*maybe_compressed_out, server_revision, block.cloneEmpty());
    }

    writeVarUInt(Protocol::Client::Data, *out);
    writeStringBinary(name, *out);

    size_t prev_bytes = out->count();

    block_out->write(block);
    maybe_compressed_out->next();
    out->next();

    if (throttler)
        throttler->add(out->count() - prev_bytes);
}


void Connection::sendPreparedData(ReadBuffer & input, size_t size, const String & name)
{
    /// NOTE 'Throttler' is not used in this method (could use, but it's not important right now).

    writeVarUInt(Protocol::Client::Data, *out);
    writeStringBinary(name, *out);

    if (0 == size)
        copyData(input, *out);
    else
        copyData(input, *out, size);
    out->next();
}


void Connection::sendExternalTablesData(ExternalTablesData & data)
{
    if (data.empty())
    {
        /// Send empty block, which means end of data transfer.
        sendData(Block());
        return;
    }

    Stopwatch watch;
    size_t out_bytes = out ? out->count() : 0;
    size_t maybe_compressed_out_bytes = maybe_compressed_out ? maybe_compressed_out->count() : 0;
    size_t rows = 0;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::SendExternalTables};

    for (auto & elem : data)
    {
        elem.first->readPrefix();
        while (Block block = elem.first->read())
        {
            rows += block.rows();
            sendData(block, elem.second);
        }
        elem.first->readSuffix();
    }

    /// Send empty block, which means end of data transfer.
    sendData(Block());

    out_bytes = out->count() - out_bytes;
    maybe_compressed_out_bytes = maybe_compressed_out->count() - maybe_compressed_out_bytes;
    double elapsed = watch.elapsedSeconds();

    std::stringstream msg;
    msg << std::fixed << std::setprecision(3);
    msg << "Sent data for " << data.size() << " external tables, total " << rows << " rows in " << elapsed << " sec., "
        << static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., "
        << maybe_compressed_out_bytes / 1048576.0 << " MiB (" << maybe_compressed_out_bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.)";

    if (compression == Protocol::Compression::Enable)
        msg << ", compressed " << static_cast<double>(maybe_compressed_out_bytes) / out_bytes << " times to "
            << out_bytes / 1048576.0 << " MiB (" << out_bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.)";
    else
        msg << ", no compression.";

    LOG_DEBUG(log_wrapper.get(), msg.rdbuf());
}

Poco::Net::SocketAddress Connection::getResolvedAddress() const
{
    if (connected)
        return current_resolved_address;

    return DNSResolver::instance().resolveAddress(host, port);
}


bool Connection::poll(size_t timeout_microseconds)
{
    return static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_microseconds);
}


bool Connection::hasReadBufferPendingData() const
{
    return static_cast<const ReadBufferFromPocoSocket &>(*in).hasPendingData();
}


Connection::Packet Connection::receivePacket()
{
    //LOG_TRACE(log_wrapper.get(), "Receiving packet");

    try
    {
        Packet res;
        readVarUInt(res.type, *in);

        switch (res.type)
        {
            case Protocol::Server::Data:
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

            case Protocol::Server::Totals:
                /// Block with total values is passed in same form as ordinary block. The only difference is packed id.
                res.block = receiveData();
                return res;

            case Protocol::Server::Extremes:
                /// Same as above.
                res.block = receiveData();
                return res;

            case Protocol::Server::EndOfStream:
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
        /// Add server address to exception message, if need.
        if (e.code() != ErrorCodes::UNKNOWN_PACKET_FROM_SERVER)
            e.addMessage("while receiving packet from " + getDescription());

        throw;
    }
}


Block Connection::receiveData()
{
    //LOG_TRACE(log_wrapper.get(), "Receiving data");

    initBlockInput();

    String external_table_name;
    readStringBinary(external_table_name, *in);

    size_t prev_bytes = in->count();

    /// Read one block from network.
    Block res = block_in->read();

    if (throttler)
        throttler->add(in->count() - prev_bytes);

    return res;
}


void Connection::initBlockInput()
{
    if (!block_in)
    {
        if (compression == Protocol::Compression::Enable)
            maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in);
        else
            maybe_compressed_in = in;

        block_in = std::make_shared<NativeBlockInputStream>(*maybe_compressed_in, server_revision);
    }
}


void Connection::setDescription()
{
    auto resolved_address = getResolvedAddress();
    description = host + ":" + toString(resolved_address.port());
    auto ip_address =  resolved_address.host().toString();

    if (host != ip_address)
        description += ", " + ip_address;
}


std::unique_ptr<Exception> Connection::receiveException()
{
    //LOG_TRACE(log_wrapper.get(), "Receiving exception");

    Exception e;
    readException(e, *in, "Received from " + getDescription());
    return std::unique_ptr<Exception>{ e.clone() };
}


Progress Connection::receiveProgress()
{
    //LOG_TRACE(log_wrapper.get(), "Receiving progress");

    Progress progress;
    progress.read(*in, server_revision);
    return progress;
}


BlockStreamProfileInfo Connection::receiveProfileInfo()
{
    BlockStreamProfileInfo profile_info;
    profile_info.read(*in);
    return profile_info;
}

void Connection::fillBlockExtraInfo(BlockExtraInfo & info) const
{
    info.is_valid = true;
    info.host = host;
    info.resolved_address = getResolvedAddress().toString();
    info.port = port;
    info.user = user;
}

void Connection::throwUnexpectedPacket(UInt64 packet_type, const char * expected) const
{
    throw NetException(
            "Unexpected packet from server " + getDescription() + " (expected " + expected
            + ", got " + String(Protocol::Server::toString(packet_type)) + ")",
            ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}

}
