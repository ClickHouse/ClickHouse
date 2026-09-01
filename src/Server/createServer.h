#pragma once

#include <functional>
#include <string>
#include <vector>

#include <base/types.h>
#include <Common/logger_useful.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

class ProtocolServerAdapter;

using CreateServerFunc = std::function<ProtocolServerAdapter(UInt16)>;

/// Try to create and optionally start a protocol server for the given listen_host / port_name pair.
/// Handles duplicate detection, config lookup, and listen_try fallback.
/// Returns true if a new server was actually added.
bool createServer(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & listen_host,
    const char * port_name,
    bool listen_try,
    bool start_server,
    std::vector<ProtocolServerAdapter> & servers,
    CreateServerFunc && func,
    LoggerRawPtr log);

/// Start the servers created by `createServer` with `start_server = false`.
/// Protocols that bind their listening socket in `start` rather than at creation (gRPC, Arrow Flight,
/// see `ProtocolServerAdapter::bindsOnStart`) can only report `EADDRINUSE` here, so `listen_try` has
/// to be handled at this point as well: such a listener is dropped with a warning instead of
/// preventing the whole server from starting up.
void startServers(std::vector<ProtocolServerAdapter> & servers, bool listen_try, LoggerRawPtr log);

}
