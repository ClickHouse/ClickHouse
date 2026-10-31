#include <Common/Config/getConfigPath.h>

#include <base/pathToString.h>

#include <filesystem>
#include <string_view>

namespace fs = std::filesystem;

namespace DB
{

/// `.conf` is deliberately not here: it is accepted for the files of a `config.d` merge directory,
/// but it never was a name of a main configuration file.
static constexpr std::string_view supported_config_extensions[] = {".xml", ".yaml", ".yml"};

std::optional<std::string> tryGetConfigPath(const std::string & path_without_extension)
{
    /// Enter `std::filesystem` through `pathFromString`: on Windows the narrow constructor would
    /// decode the UTF-8 name through the active code page.
    const fs::path base_path = pathFromString(path_without_extension);
    for (const auto & extension : supported_config_extensions)
    {
        fs::path config_path = base_path;
        config_path += extension;

        std::error_code ec;
        if (fs::exists(config_path, ec))
            return pathToGenericString(config_path);
    }

    return std::nullopt;
}

std::string getConfigPathForAnySupportedFormat(const std::string & path)
{
    return tryGetConfigPath(pathToString(pathFromString(path).replace_extension())).value_or(path);
}

}
