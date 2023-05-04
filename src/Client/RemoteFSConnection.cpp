#include <Client/RemoteFSConnection.h>
#include <Common/DNSResolver.h>
#include <Common/NetException.h>

#include "Core/RemoteFSProtocol.h"

#include <IO/ReadHelpers.h>
#include <IO/TimeoutSetter.h>
#include <IO/WriteHelpers.h>

#include <Poco/Net/NetException.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

RemoteFSConnection::RemoteFSConnection(const String & host_, UInt16 port_, const String & disk_name_, size_t conn_id_)
    : host(host_), port(port_), disk_name(disk_name_), conn_id(conn_id_), log_wrapper(*this)
{
    /// Don't connect immediately, only on first need.

    setDescription();
}

RemoteFSConnection::~RemoteFSConnection()
{
    LOG_TRACE(log_wrapper.get(), "Connection with remote disk {} destroyed", disk_name);
}

void RemoteFSConnection::connect(const ConnectionTimeouts & timeouts)
{
    try
    {
        LOG_TRACE(log_wrapper.get(), "Connecting. Disk: {}", disk_name);

        auto addresses = DNSResolver::instance().resolveAddressList(host, port);
        const auto & connection_timeout = timeouts.connection_timeout;

        for (auto it = addresses.begin(); it != addresses.end();)
        {
            if (connected)
                disconnect();

            socket = std::make_unique<Poco::Net::StreamSocket>();

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
        int tcp_keep_alive_timeout_in_sec = timeouts.tcp_keep_alive_timeout.totalSeconds();
        if (tcp_keep_alive_timeout_in_sec)
        {
            socket->setKeepAlive(true);
            socket->setOption(
                IPPROTO_TCP,
#if defined(TCP_KEEPALIVE)
                TCP_KEEPALIVE
#else
                TCP_KEEPIDLE // __APPLE__
#endif
                ,
                tcp_keep_alive_timeout_in_sec);
        }

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;

        sendHello();
        receiveHello();

        LOG_TRACE(log_wrapper.get(), "Connected to remote disk {}", disk_name);
    }
    catch (Poco::Net::NetException & e)
    {
        disconnect();

        /// Remove this possible stale entry from cache
        DNSResolver::instance().removeHostFromCache(host);

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        throw NetException(ErrorCodes::NETWORK_ERROR, "{} ({})", e.displayText(), getDescription());
    }
    catch (Poco::TimeoutException & e)
    {
        disconnect();

        /// Remove this possible stale entry from cache
        DNSResolver::instance().removeHostFromCache(host);

        /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
        /// This exception can only be thrown from socket->connect(), so add information about connection timeout.
        const auto & connection_timeout = timeouts.connection_timeout;
        throw NetException(
            ErrorCodes::SOCKET_TIMEOUT,
            "{} ({}, connection timeout {} ms)",
            e.displayText(),
            getDescription(),
            connection_timeout.totalMilliseconds());
    }
}

void RemoteFSConnection::disconnect()
{
    in = nullptr;
    out = nullptr;
    if (socket)
        socket->close();
    socket = nullptr;
    connected = false;
}

void RemoteFSConnection::forceConnected(const ConnectionTimeouts & timeouts)
{
    if (!connected)
    {
        connect(timeouts);
    }
    else if (!ping(timeouts))
    {
        LOG_TRACE(log_wrapper.get(), "Connection was closed, will reconnect.");
        connect(timeouts);
    }
}

const String & RemoteFSConnection::getDescription() const
{
    return description;
}

const String & RemoteFSConnection::getHost() const
{
    return host;
}

UInt16 RemoteFSConnection::getPort() const
{
    return port;
}

std::optional<Poco::Net::SocketAddress> RemoteFSConnection::getResolvedAddress() const
{
    return current_resolved_address;
}

void RemoteFSConnection::setDescription()
{
    auto resolved_address = getResolvedAddress();
    description = host + ":" + toString(port) + "/" + disk_name + "?id=" + toString(conn_id);

    if (resolved_address)
    {
        auto ip_address = resolved_address->host().toString();
        if (host != ip_address)
            description += ", " + ip_address;
    }
}

UInt64 RemoteFSConnection::getTotalSpace()
{
    LOG_TRACE(log_wrapper.get(), "Send GetTotalSpace");
    writeVarUInt(RemoteFSProtocol::GetTotalSpace, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::GetTotalSpace, "GetTotalSpace");

    UInt64 res;
    readVarUInt(res, *in);
    return res;
}

UInt64 RemoteFSConnection::getAvailableSpace()
{
    LOG_TRACE(log_wrapper.get(), "Send GetAvailableSpace");
    writeVarUInt(RemoteFSProtocol::GetAvailableSpace, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::GetAvailableSpace, "GetAvailableSpace");

    UInt64 res;
    readVarUInt(res, *in);
    return res;
}

bool RemoteFSConnection::exists(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send Exists");
    writeVarUInt(RemoteFSProtocol::Exists, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::Exists, "Exists");

    bool res;
    readBoolText(res, *in);
    return res;
}

bool RemoteFSConnection::isFile(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send IsFile");
    writeVarUInt(RemoteFSProtocol::IsFile, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::IsFile, "IsFile");

    bool res;
    readBoolText(res, *in);
    return res;
}

bool RemoteFSConnection::isDirectory(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send IsDirectory");
    writeVarUInt(RemoteFSProtocol::IsDirectory, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::IsDirectory, "IsDirectory");

    bool res;
    readBoolText(res, *in);
    return res;
}

size_t RemoteFSConnection::getFileSize(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send GetFileSize");
    writeVarUInt(RemoteFSProtocol::GetFileSize, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::GetFileSize, "GetFileSize");

    size_t res;
    readVarUInt(res, *in);
    return res;
}

void RemoteFSConnection::createDirectory(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send CreateDirectory");
    writeVarUInt(RemoteFSProtocol::CreateDirectory, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::CreateDirectory, "CreateDirectory");
}

void RemoteFSConnection::createDirectories(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send CreateDirectories");
    writeVarUInt(RemoteFSProtocol::CreateDirectories, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::CreateDirectories, "CreateDirectories");
}

void RemoteFSConnection::clearDirectory(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send ClearDirectory");
    writeVarUInt(RemoteFSProtocol::ClearDirectory, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::ClearDirectory, "ClearDirectory");
}

void RemoteFSConnection::moveDirectory(const String & from_path, const String & to_path)
{
    LOG_TRACE(log_wrapper.get(), "Send MoveDirectory");
    writeVarUInt(RemoteFSProtocol::MoveDirectory, *out);
    writeStringBinary(from_path, *out);
    writeStringBinary(to_path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::MoveDirectory, "MoveDirectory");
}

void RemoteFSConnection::startIterateDirectory(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send StartIterateDirectory");
    writeVarUInt(RemoteFSProtocol::StartIterateDirectory, *out);
    writeStringBinary(path, *out);
    out->next();
}

bool RemoteFSConnection::nextDirectoryIteratorEntry(String & entry)
{
    UInt64 packet_type;
    readVarUInt(packet_type, *in);
    switch (packet_type)
    {
        case RemoteFSProtocol::DataPacket:
            readStringBinary(entry, *in);
            return true;
        case RemoteFSProtocol::EndIterateDirectory:
            return false;
        case RemoteFSProtocol::Exception:
            receiveException()->rethrow();
            break;
        default:
            /// Close connection, to not stay in unsynchronised state.
            disconnect();
            throwUnexpectedPacket(packet_type, "DataPacket or EndIterateDirectory");
    }
    return false;
}

void RemoteFSConnection::createFile(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send CreateFile");
    writeVarUInt(RemoteFSProtocol::CreateFile, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::CreateFile, "CreateFile");
}

void RemoteFSConnection::moveFile(const String & from_path, const String & to_path)
{
    LOG_TRACE(log_wrapper.get(), "Send MoveFile");
    writeVarUInt(RemoteFSProtocol::MoveFile, *out);
    writeStringBinary(from_path, *out);
    writeStringBinary(to_path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::MoveFile, "MoveFile");
}

void RemoteFSConnection::replaceFile(const String & from_path, const String & to_path)
{
    LOG_TRACE(log_wrapper.get(), "Send ReplaceFile");
    writeVarUInt(RemoteFSProtocol::ReplaceFile, *out);
    writeStringBinary(from_path, *out);
    writeStringBinary(to_path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::ReplaceFile, "ReplaceFile");
}

void RemoteFSConnection::copy(const String & from_path, const String & to_path)
{
    LOG_TRACE(log_wrapper.get(), "Send Copy");
    writeVarUInt(RemoteFSProtocol::Copy, *out);
    writeStringBinary(from_path, *out);
    writeStringBinary(to_path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::Copy, "Copy");
}

void RemoteFSConnection::copyDirectoryContent(const String & from_dir, const String & to_dir)
{
    LOG_TRACE(log_wrapper.get(), "Send CopyDirectoryContent");
    writeVarUInt(RemoteFSProtocol::CopyDirectoryContent, *out);
    writeStringBinary(from_dir, *out);
    writeStringBinary(to_dir, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::CopyDirectoryContent, "CopyDirectoryContent");
}

void RemoteFSConnection::listFiles(const String & path, std::vector<String> & file_names)
{
    LOG_TRACE(log_wrapper.get(), "Send ListFiles");
    writeVarUInt(RemoteFSProtocol::ListFiles, *out);
    writeStringBinary(path, *out);
    out->next();

    size_t count;
    readVarUInt(count, *in);
    file_names.clear();
    file_names.reserve(count);
    for (size_t i = 0; i < count; i++)
    {
        receiveAndCheckPacketType(RemoteFSProtocol::DataPacket, "DataPacket");
        String entry;
        readStringBinary(entry, *in);
        file_names.emplace_back(entry);
    }
}

size_t RemoteFSConnection::readFile(const String & path, size_t offset, size_t size, char * data)
{
    LOG_TRACE(log_wrapper.get(), "Send ReadFile");
    writeVarUInt(RemoteFSProtocol::ReadFile, *out);
    writeStringBinary(path, *out);
    writeVarUInt(offset, *out);
    writeVarUInt(size, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::ReadFile, "ReadFile");
    readVarUInt(size, *in);
    in->readStrict(data, size);
    return size;
}

void RemoteFSConnection::startWriteFile(const String & path, size_t buf_size, WriteMode mode)
{
    LOG_TRACE(log_wrapper.get(), "Send StartWriteFile");
    writeVarUInt(RemoteFSProtocol::StartWriteFile, *out);
    writeStringBinary(path, *out);
    writeVarUInt(buf_size, *out);
    writeVarUInt(UInt64(mode), *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::StartWriteFile, "StartWriteFile");
}

void RemoteFSConnection::writeData(const char * data, size_t size)
{
    LOG_TRACE(log_wrapper.get(), "Send DataPacket");
    writeVarUInt(RemoteFSProtocol::DataPacket, *out);
    writeVarUInt(size, *out);
    out->write(data, size);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::DataPacket, "DataPacket");
}

void RemoteFSConnection::endWriteFile()
{
    LOG_TRACE(log_wrapper.get(), "Send EndWriteFile");
    writeVarUInt(RemoteFSProtocol::EndWriteFile, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::EndWriteFile, "EndWriteFile");
}

void RemoteFSConnection::removeFile(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send RemoveFile");
    writeVarUInt(RemoteFSProtocol::RemoveFile, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::RemoveFile, "RemoveFile");
}

void RemoteFSConnection::removeFileIfExists(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send RemoveFileIfExists");
    writeVarUInt(RemoteFSProtocol::RemoveFileIfExists, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::RemoveFileIfExists, "RemoveFileIfExists");
}

void RemoteFSConnection::removeDirectory(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send RemoveDirectory");
    writeVarUInt(RemoteFSProtocol::RemoveDirectory, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::RemoveDirectory, "RemoveDirectory");
}

void RemoteFSConnection::removeRecursive(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send RemoveRecursive");
    writeVarUInt(RemoteFSProtocol::RemoveRecursive, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::RemoveRecursive, "RemoveRecursive");
}

void RemoteFSConnection::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    LOG_TRACE(log_wrapper.get(), "Send SetLastModified");
    writeVarUInt(RemoteFSProtocol::SetLastModified, *out);
    writeStringBinary(path, *out);
    writeVarUInt(timestamp.epochTime(), *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::SetLastModified, "SetLastModified");
}

Poco::Timestamp RemoteFSConnection::getLastModified(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send GetLastModified");
    writeVarUInt(RemoteFSProtocol::GetLastModified, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::GetLastModified, "GetLastModified");
    time_t epoch_time;
    readVarUInt(epoch_time, *in);
    return Poco::Timestamp::fromEpochTime(epoch_time);
}

time_t RemoteFSConnection::getLastChanged(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send GetLastChanged");
    writeVarUInt(RemoteFSProtocol::GetLastChanged, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::GetLastChanged, "GetLastChanged");
    time_t res;
    readVarUInt(res, *in);
    return res;
}

void RemoteFSConnection::setReadOnly(const String & path)
{
    LOG_TRACE(log_wrapper.get(), "Send SetReadOnly");
    writeVarUInt(RemoteFSProtocol::SetReadOnly, *out);
    writeStringBinary(path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::SetReadOnly, "SetReadOnly");
}

void RemoteFSConnection::createHardLink(const String & src_path, const String & dst_path)
{
    LOG_TRACE(log_wrapper.get(), "Send CreateHardLink");
    writeVarUInt(RemoteFSProtocol::CreateHardLink, *out);
    writeStringBinary(src_path, *out);
    writeStringBinary(dst_path, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::CreateHardLink, "CreateHardLink");
}

void RemoteFSConnection::truncateFile(const String & path, size_t size)
{
    LOG_TRACE(log_wrapper.get(), "Send TruncateFile");
    writeVarUInt(RemoteFSProtocol::TruncateFile, *out);
    writeStringBinary(path, *out);
    writeVarUInt(size, *out);
    out->next();

    receiveAndCheckPacketType(RemoteFSProtocol::TruncateFile, "TruncateFile");
}

bool RemoteFSConnection::ping(const ConnectionTimeouts & timeouts)
{
    try
    {
        TimeoutSetter timeout_setter(*socket, timeouts.sync_request_timeout, true);

        UInt64 pong = 0;
        LOG_TRACE(log_wrapper.get(), "Send ping");
        writeVarUInt(RemoteFSProtocol::Ping, *out);
        out->next();

        if (in->eof())
            return false;

        readVarUInt(pong, *in);

        if (pong != RemoteFSProtocol::Pong)
            throwUnexpectedPacket(pong, "Pong");
        LOG_TRACE(log_wrapper.get(), "Got pong");
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

void RemoteFSConnection::sendHello()
{
    writeVarUInt(RemoteFSProtocol::Hello, *out);
    writeStringBinary(disk_name, *out);
    out->next();
}

void RemoteFSConnection::receiveHello()
{
    /// Receive hello packet.
    UInt64 packet_type = 0;

    /// Prevent read after eof in readVarUInt in case of reset connection
    /// (Poco should throw such exception while reading from socket but
    /// sometimes it doesn't for unknown reason)
    if (in->eof())
        throw Poco::Net::NetException("Connection reset by peer");

    readVarUInt(packet_type, *in);
    if (packet_type == RemoteFSProtocol::Hello)
    {
        return;
    }
    else if (packet_type == RemoteFSProtocol::Exception)
        receiveException()->rethrow();
    else
    {
        /// Close connection, to not stay in unsynchronised state.
        disconnect();
        throwUnexpectedPacket(packet_type, "Hello or Exception");
    }
}

void RemoteFSConnection::receiveAndCheckPacketType(UInt64 expected_packet_type, const char * expected_packet_name)
{
    UInt64 packet_type;
    readVarUInt(packet_type, *in);
    LOG_TRACE(log_wrapper.get(), "Received {}", RemoteFSProtocol::PacketType(packet_type));
    if (packet_type == expected_packet_type)
    {
        return;
    }
    else if (packet_type == RemoteFSProtocol::Exception)
    {
        receiveException()->rethrow();
    }
    else
    {
        /// Close connection, to not stay in unsynchronised state.
        disconnect();
        throwUnexpectedPacket(packet_type, expected_packet_name);
    }
}

std::unique_ptr<Exception> RemoteFSConnection::receiveException() const
{
    return std::make_unique<Exception>(readException(*in, "Received from " + getDescription(), true /* remote */));
}

void RemoteFSConnection::throwUnexpectedPacket(UInt64 packet_type, const char * expected) const
{
    throw NetException(
        ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
        "Unexpected packet from server {} (expected {}, got {})",
        getDescription(),
        expected,
        packet_type);
}

}
