#pragma once

#include <vector>

#include <Common/logger_useful.h>

namespace DB
{

class ProtocolServerAdapter;
class ServerType;

/// Stop protocol servers that match the given server type.
/// Removes servers from the vector once all their connections are closed.
void stopServers(std::vector<ProtocolServerAdapter> & servers, const ServerType & server_type, LoggerRawPtr log);

}
