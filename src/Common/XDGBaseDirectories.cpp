#include <Common/XDGBaseDirectories.h>

#include <Common/getUserHomePath.h>

#include <base/pathToString.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace
{
    constexpr const char* ENV_XDG_CONFIG_HOME = "XDG_CONFIG_HOME";
    constexpr const char* CONFIG_PATH_PREFIX = ".config";

    constexpr const char* ENV_XDG_DATA_HOME = "XDG_DATA_HOME";
    constexpr const char* DATA_PATH_PREFIX = ".local/share";

    constexpr const char* ENV_XDG_STATE_HOME = "XDG_STATE_HOME";
    constexpr const char* STATE_PATH_PREFIX = ".local/state";

    constexpr const char* ENV_XDG_CACHE_HOME = "XDG_STATE_HOME";
    constexpr const char* CACHE_PATH_PREFIX = ".cache";

    constexpr const char* APP_NAME = "clickhouse";

    fs::path getPathFromEnvOrDefault(const char* env_var_name, const char* path_prefix)
    {
        /// Read through the wide environment on Windows and construct the path from UTF-8:
        /// `getenv` + `fs::path(const char *)` would round-trip through the active code page
        /// and mangle any component outside it.
        auto xdg_config_home = getPathFromEnvironment(env_var_name);
        if (!xdg_config_home.empty())
            return pathFromString(xdg_config_home) / APP_NAME;

        /// Not plain `$HOME`: a native Windows shell names the home directory differently
        /// (see getUserHomePath), and without this the client has no default config, history
        /// or cache location there at all.
        auto home_path = getUserHomePath();
        if (!home_path.empty())
            return pathFromString(home_path) / path_prefix / APP_NAME;

        return "";
    }
}

fs::path XDGBaseDirectories::getConfigurationHome()
{
    return getPathFromEnvOrDefault(ENV_XDG_CONFIG_HOME, CONFIG_PATH_PREFIX);
}

fs::path XDGBaseDirectories::getDataHome()
{
    return getPathFromEnvOrDefault(ENV_XDG_DATA_HOME, DATA_PATH_PREFIX);
}

fs::path XDGBaseDirectories::getStateHome()
{
    return getPathFromEnvOrDefault(ENV_XDG_STATE_HOME, STATE_PATH_PREFIX);
}

fs::path XDGBaseDirectories::getCacheHome()
{
    return getPathFromEnvOrDefault(ENV_XDG_CACHE_HOME, CACHE_PATH_PREFIX);
}

}
