#pragma once

#include <configuration.h>

#include <string>

namespace apsara::odps::sdk::clickhouse
{

/// Resolve the Tunnel endpoint through the SDK's signed ODPS routing request.
/// ClickHouse validates the returned endpoint before constructing a download.
std::string resolveTunnelEndpoint(const Configuration & configuration, const std::string & project);

}
