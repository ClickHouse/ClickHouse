#include <Common/Config/getClientConfigPath.h>

#include <Common/Config/getConfigPath.h>
#include <Common/XDGBaseDirectories.h>
#include <base/pathToString.h>

#include <vector>


namespace DB
{

std::optional<std::string> getClientConfigPath(const std::string & home_path)
{
    /// The candidates are `std::filesystem::path`: on Windows, building a `path` from a byte string
    /// mangles anything outside the active code page just as reading one back out does, so the
    /// UTF-8 boundary is crossed only through `pathFromString` and `pathToString`, here and in
    /// `tryGetConfigPath`.
    std::vector<fs::path> names;
    names.emplace_back("./clickhouse-client");

    auto xdg_config_home = XDGBaseDirectories::getConfigurationHome();
    if (!xdg_config_home.empty())
        names.emplace_back(xdg_config_home / "config");

    if (!home_path.empty())
        names.emplace_back(pathFromString(home_path) / ".clickhouse-client" / "config");

    names.emplace_back("/etc/clickhouse-client/config");

    for (const auto & name : names)
        if (auto config_path = tryGetConfigPath(pathToString(name)))
            return config_path;

    return std::nullopt;
}

}
