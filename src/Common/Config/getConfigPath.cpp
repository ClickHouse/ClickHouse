#include <Common/Config/getConfigPath.h>

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
    for (const auto & extension : supported_config_extensions)
    {
        std::string config_path = path_without_extension + std::string(extension);

        std::error_code ec;
        if (fs::exists(config_path, ec))
            return config_path;
    }

    return std::nullopt;
}

std::string getConfigPathForAnySupportedFormat(const std::string & path)
{
    return tryGetConfigPath(fs::path(path).replace_extension().string()).value_or(path);
}

}
