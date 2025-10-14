#include "Common/XDGBaseDirectories.h"

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

std::string XDGBaseDirectories::getConfigurationHome()
{
    auto * xdg_config_home = getenv(ENV_XDG_CONFIG_HOME); // NOLINT(concurrency-mt-unsafe)
    if (xdg_config_home)
        return fs::path(xdg_config_home) / APP_NAME;

    auto * home_path = getenv(ENV_HOME); // NOLINT(concurrency-mt-unsafe)
    if (home_path)
        return fs::path(home_path) / CONFIG_PATH_PREFIX / APP_NAME;

    return "";
}

std::string XDGBaseDirectories::getDataHome()
{
    auto * xdg_data_home = getenv(ENV_XDG_DATA_HOME); // NOLINT(concurrency-mt-unsafe)
    if (xdg_data_home)
        return fs::path(xdg_data_home) / APP_NAME;

    auto * home_path = getenv(ENV_HOME); // NOLINT(concurrency-mt-unsafe)
    if (home_path)
        return fs::path(home_path) / DATA_PATH_PREFIX / APP_NAME;

    return "";
}

std::string XDGBaseDirectories::getStateHome()
{
    auto * xdg_data_home = getenv(ENV_XDG_STATE_HOME); // NOLINT(concurrency-mt-unsafe)
    if (xdg_data_home)
        return fs::path(xdg_data_home) / APP_NAME;

    auto * home_path = getenv(ENV_HOME); // NOLINT(concurrency-mt-unsafe)
    if (home_path)
        return fs::path(home_path) / STATE_PATH_PREFIX / APP_NAME;

    return "";
}

}
