#include <Common/Config/getClientConfigPath.h>
#include <Common/XDGBaseDirectories.h>
#include <base/pathToString.h>

#include <filesystem>
#include <vector>


namespace fs = std::filesystem;

namespace DB
{

std::optional<std::string> getClientConfigPath(const std::string & home_path)
{
    /// The candidates stay `std::filesystem::path` throughout: on Windows, building a `path` from a
    /// byte string mangles anything outside the active code page just as reading one back out does,
    /// so the UTF-8 boundary is crossed exactly once on each side - `pathFromString` for the
    /// incoming home directory, `pathToGenericString` for the path we return.
    std::vector<fs::path> names;
    names.emplace_back("./clickhouse-client");

    auto xdg_config_home = XDGBaseDirectories::getConfigurationHome();
    if (!xdg_config_home.empty())
        names.emplace_back(xdg_config_home / "config");

    if (!home_path.empty())
        names.emplace_back(pathFromString(home_path) / ".clickhouse-client" / "config");

    names.emplace_back("/etc/clickhouse-client/config");

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
