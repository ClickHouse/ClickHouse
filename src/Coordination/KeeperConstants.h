#pragma once

#include <IO/WriteHelpers.h>

namespace DB
{

/// left for backwards compatibility
enum class KeeperApiVersion : uint8_t
{
    ZOOKEEPER_COMPATIBLE = 0,
    WITH_FILTERED_LIST,
    WITH_MULTI_READ,
    WITH_CHECK_NOT_EXISTS,
};

const std::string keeper_system_path = "/keeper";
const std::string keeper_api_version_path = keeper_system_path + "/api_version";
const std::string keeper_api_feature_flags_path = keeper_system_path + "/feature_flags";

}
