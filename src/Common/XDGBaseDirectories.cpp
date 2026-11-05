#include <Common/XDGBaseDirectories.h>

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

    constexpr const char* ENV_HOME = "HOME";

    constexpr const char* APP_NAME = "clickhouse";

    fs::path getPathFromEnvOrDefault(const char* env_var_name, const char* path_prefix)
    {
        auto * xdg_config_home = getenv(env_var_name); // NOLINT(concurrency-mt-unsafe)
        if (xdg_config_home)
            return fs::path(xdg_config_home) / APP_NAME;

        auto * home_path = getenv(ENV_HOME); // NOLINT(concurrency-mt-unsafe)
        if (home_path)
            return fs::path(home_path) / path_prefix / APP_NAME;

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
