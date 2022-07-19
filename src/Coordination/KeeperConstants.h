#pragma once

namespace DB
{

const std::string keeper_system_path = "/keeper";
const std::string keeper_api_version_path = keeper_system_path + "/api_version";

enum class KeeperApiVersion : uint8_t
{
    V0 = 0, // ZooKeeper compatible version
    V1      // added FilteredList request
};

inline constexpr auto current_keeper_api_version = KeeperApiVersion::V1;

}
