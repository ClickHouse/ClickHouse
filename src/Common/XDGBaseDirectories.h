#pragma once

#include <cstdlib>
#include <string>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

/// https://specifications.freedesktop.org/basedir-spec/latest/
class XDGBaseDirectories
{
    static constexpr const char* ENV_XDG_CONFIG_HOME = "XDG_CONFIG_HOME";
    static constexpr const char* ENV_HOME = "HOME";

public:
    static std::string getConfigurationHome()
    {
        auto * xdg_config_home = getenv(ENV_XDG_CONFIG_HOME); // NOLINT(concurrency-mt-unsafe)
        if (xdg_config_home)
            return fs::path(xdg_config_home) / "clickhouse";

        auto * home_path = getenv(ENV_HOME); // NOLINT(concurrency-mt-unsafe)
        if (home_path)
            return std::string(home_path) + "/.config/clickhouse";

        return "";
    }

};

}
