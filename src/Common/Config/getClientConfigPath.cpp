#include <Common/Config/getClientConfigPath.h>
#include <Common/XDGBaseDirectories.h>

#include <filesystem>
#include <vector>


namespace fs = std::filesystem;

namespace DB
{

std::optional<std::string> getClientConfigPath(const std::string & home_path)
{
    std::string config_path;

    std::vector<std::string> names;
    names.emplace_back("./clickhouse-client");

    auto xdg_config_home = XDGBaseDirectories::getConfigurationHome();
    if (!xdg_config_home.empty())
        names.emplace_back(xdg_config_home / "config");

    if (!home_path.empty())
        names.emplace_back(home_path + "/.clickhouse-client/config");

    names.emplace_back("/etc/clickhouse-client/config");

    for (const auto & name : names)
    {
        for (const auto & extension : {".xml", ".yaml", ".yml"})
        {
            config_path = name + extension;

            std::error_code ec;
            if (fs::exists(config_path, ec))
                return config_path;
        }
    }

    return std::nullopt;
}

}
