#pragma once

#include <base/types.h>

#include <string_view>

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

/// `constexpr` on purpose: this header is included by hundreds of translation units, and a `const String`
/// here would give each of them a static initializer.
constexpr std::string_view keeper_system_path = "/keeper";
constexpr std::string_view keeper_api_version_path = "/keeper/api_version";
constexpr std::string_view keeper_api_feature_flags_path = "/keeper/feature_flags";
constexpr std::string_view keeper_config_path = "/keeper/config";
constexpr std::string_view keeper_max_request_size_path = "/keeper/max_request_size";
constexpr std::string_view keeper_availability_zone_path = "/keeper/availability_zone";
const Int64 keeper_internal_get_session_id = -1;
const Int64 keeper_internal_ttl_garbage_collector_session_id = -2;
const Int64 keeper_internal_container_garbage_collector_session_id = -3;
constexpr Int64 DEFAULT_KEEPER_SNAPSHOT_ZSTD_COMPRESSION_LEVEL = 3;

/// Maximum allowed TTL value in milliseconds, matching ZooKeeper's
/// `EphemeralType.TTL.maxValue()`. Prevents `time + ttl` from overflowing
/// signed 64-bit when computing destroy_time.
constexpr Int64 MAX_KEEPER_TTL_MS = 0x00FFFFFFFFFFFFFFLL;

}
