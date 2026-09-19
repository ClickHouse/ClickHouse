#include <Common/Config/getLocalConfigPath.h>
#include <base/pathToString.h>

#include <filesystem>
#include <vector>


namespace fs = std::filesystem;

namespace DB
{

std::optional<std::string> getLocalConfigPath(const std::string & home_path)
{
    /// As in `getClientConfigPath`: the candidates stay `std::filesystem::path`, so that the
    /// UTF-8 home directory is not round-tripped through the Windows active code page.
    std::vector<fs::path> names;
    names.emplace_back("./clickhouse-local");

    if (!home_path.empty())
        names.emplace_back(pathFromString(home_path) / ".clickhouse-local" / "config");

    names.emplace_back("/etc/clickhouse-local/config");

    for (const auto & name : names)
    {
        for (const auto & extension : {".xml", ".yaml", ".yml"})
        {
            fs::path config_path = name;
            config_path += extension;

            std::error_code ec;
            if (fs::exists(config_path, ec))
                return pathToGenericString(config_path);
        }
    }

    return std::nullopt;
}

}
