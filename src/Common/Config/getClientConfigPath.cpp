#include <Common/Config/getClientConfigPath.h>

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
    const char* xdg_config_home = std::getenv("XDG_CONFIG_HOME"); // NOLINT(concurrency-mt-unsafe)
    if (xdg_config_home && xdg_config_home[0] != '\0')
    {
        names.emplace_back(std::string(xdg_config_home) + "/clickhouse-client/config");
    }
    if (!home_path.empty())
    {
        names.emplace_back(home_path + "/.config/clickhouse-client/config");
        names.emplace_back(home_path + "/.clickhouse-client/config");
    }
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
