#pragma once

#include <Poco/Net/SocketAddress.h>
#include <Common/Logger.h>

namespace Poco { class Logger; }

namespace DB
{

/// Resolves an address of this host to bind a socket to.
///
/// A literal IP address is used as is: there is no name to look up, and the
/// `dns_allow_resolve_names_to_ipv4` / `dns_allow_resolve_names_to_ipv6` settings restrict which
/// addresses we resolve *names of remote hosts* to, not which of our own addresses we may bind.
/// A host name is resolved through `DNSResolver`, so the lookup is cached and counted like every
/// other name resolution in the server.
Poco::Net::SocketAddress makeBindAddress(const std::string & host, uint16_t port);

/// The same for `listen_host`, with a hint in the log when the name cannot be resolved.
Poco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, Poco::Logger * log);

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, LoggerPtr log);

}
