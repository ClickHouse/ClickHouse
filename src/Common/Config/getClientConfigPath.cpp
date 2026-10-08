#include <Common/Config/getClientConfigPath.h>

#include <Common/Config/getConfigPath.h>
#include <Common/XDGBaseDirectories.h>

#include <vector>


namespace DB
{

std::optional<std::string> getClientConfigPath(const std::string & home_path)
{
    std::vector<std::string> names;
    names.emplace_back("./clickhouse-client");

    auto xdg_config_home = XDGBaseDirectories::getConfigurationHome();
    if (!xdg_config_home.empty())
        names.emplace_back(xdg_config_home / "config");

    if (!home_path.empty())
        names.emplace_back(home_path + "/.clickhouse-client/config");

    names.emplace_back("/etc/clickhouse-client/config");

    for (const auto & name : names)
        if (auto config_path = tryGetConfigPath(name))
            return config_path;

    return std::nullopt;
}

}
