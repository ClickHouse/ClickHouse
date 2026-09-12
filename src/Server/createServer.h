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

}
