#pragma once

#include <string>

#include <Common/logger_useful.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>

namespace DB
{

struct ServerSettings;

/// Bind a server socket to the given host:port and start listening.
/// If port is 0, the OS will assign an available port and the actual address is returned.
Poco::Net::SocketAddress socketBindListen(
    const ServerSettings & server_settings,
    Poco::Net::ServerSocket & socket,
    const std::string & host,
    UInt16 port,
    LoggerRawPtr log);

}
