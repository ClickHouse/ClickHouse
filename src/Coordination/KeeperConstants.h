#pragma once

#include <base/types.h>

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
    WITH_CHECK_STAT,
    WITH_GET_RECURSIVE,
};

const String keeper_system_path = "/keeper";
const String keeper_api_version_path = keeper_system_path + "/api_version";
const String keeper_api_feature_flags_path = keeper_system_path + "/feature_flags";
const String keeper_config_path = keeper_system_path + "/config";
const String keeper_availability_zone_path = keeper_system_path + "/availability_zone";
const Int64 keeper_internal_get_session_id = -1;
const Int64 keeper_internal_ttl_garbage_collector_session_id = -2;

/// Maximum allowed TTL value in milliseconds, matching ZooKeeper's
/// `EphemeralType.TTL.maxValue()`. Prevents `time + ttl` from overflowing
/// signed 64-bit when computing destroy_time.
constexpr Int64 MAX_KEEPER_TTL_MS = 0x00FFFFFFFFFFFFFFLL;

}
