#include <Common/Config/getLocalConfigPath.h>

#include <Common/Config/getConfigPath.h>

#include <vector>


namespace DB
{

std::optional<std::string> getLocalConfigPath(const std::string & home_path)
{
    std::vector<std::string> names;
    names.emplace_back("./clickhouse-local");
    if (!home_path.empty())
        names.emplace_back(home_path + "/.clickhouse-local/config");
    names.emplace_back("/etc/clickhouse-local/config");

    for (const auto & name : names)
        if (auto config_path = tryGetConfigPath(name))
            return config_path;

    return std::nullopt;
}

}
