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
    WITH_REMOVE_RECURSIVE,
};

const String keeper_system_path = "/keeper";
const String keeper_api_version_path = keeper_system_path + "/api_version";
const String keeper_api_feature_flags_path = keeper_system_path + "/feature_flags";
const String keeper_config_path = keeper_system_path + "/config";
const String keeper_availability_zone_path = keeper_system_path + "/availability_zone";

}
