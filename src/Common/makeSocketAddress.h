#pragma once

#include <Poco/Net/SocketAddress.h>
#include <Common/Logger.h>

namespace Poco { class Logger; }

namespace DB
{

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, Poco::Logger * log);

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, LoggerPtr log);

}
