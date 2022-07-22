#pragma once

#include <IO/WriteHelpers.h>

namespace DB
{

enum class KeeperApiVersion : uint8_t
{
    V0 = 0, // ZooKeeper compatible version
    V1      // added FilteredList request
};

inline constexpr auto current_keeper_api_version = KeeperApiVersion::V1;

const std::string keeper_system_path = "/keeper";
const std::string keeper_api_version_path = keeper_system_path + "/api_version";

using PathWithData = std::pair<std::string_view, std::string>;
const std::vector<PathWithData> data_for_system_paths
{
    {keeper_api_version_path, toString(static_cast<uint8_t>(current_keeper_api_version))}
};

}
