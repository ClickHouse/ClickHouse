#include <Common/Config/getLocalConfigPath.h>

#include <filesystem>
#include <vector>


namespace fs = std::filesystem;

namespace DB
{

std::optional<std::string> getLocalConfigPath(const std::string & home_path)
{
    std::string config_path;

    std::vector<std::string> names;
    names.emplace_back("./clickhouse-local");
    if (!home_path.empty())
        names.emplace_back(home_path + "/.clickhouse-local/config");
    names.emplace_back("/etc/clickhouse-local/config");

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
