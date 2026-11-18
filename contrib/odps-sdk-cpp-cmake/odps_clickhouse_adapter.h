#pragma once

#include <configuration.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace apsara::odps::sdk
{
class ODPSTableRecord;
}

namespace apsara::odps::sdk::clickhouse
{

/// Resolve the Tunnel endpoint through the SDK's signed ODPS routing request.
/// ClickHouse validates the returned endpoint before constructing a download.
std::string resolveTunnelEndpoint(const Configuration & configuration, const std::string & project);

/// Preserve the SDK API spelling behind a ClickHouse-style name without copying the value.
const char * getJSONValue(const ODPSTableRecord & record, uint32_t idx, uint32_t & length);

/// Match the SDK's exact exception prefix; changing its spelling would break retry classification.
bool isArrowDeserializeError(std::string_view message);

}
