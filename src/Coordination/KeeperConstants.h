#pragma once

#include <IO/WriteHelpers.h>

namespace DB
{

enum class KeeperApiVersion : uint8_t
{
    ZOOKEEPER_COMPATIBLE = 0,
    WITH_FILTERED_LIST
};

inline constexpr auto current_keeper_api_version = KeeperApiVersion::WITH_FILTERED_LIST;

const std::string keeper_system_path = "/keeper";
const std::string keeper_api_version_path = keeper_system_path + "/api_version";

using PathWithData = std::pair<std::string_view, std::string>;
const std::vector<PathWithData> child_system_paths_with_data
{
    {keeper_api_version_path, toString(static_cast<uint8_t>(current_keeper_api_version))}
};

}
