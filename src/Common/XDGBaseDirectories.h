#pragma once

#include <cstdlib>
#include <string>


namespace DB
{

/// https://specifications.freedesktop.org/basedir-spec/latest/
class XDGBaseDirectories
{
    static constexpr const char* ENV_XDG_CONFIG_HOME = "XDG_CONFIG_HOME";
    static constexpr const char* CONFIG_PATH_PREFIX = ".config";

    static constexpr const char* ENV_XDG_DATA_HOME = "XDG_DATA_HOME";
    static constexpr const char* DATA_PATH_PREFIX = ".local/share";

    static constexpr const char* ENV_XDG_STATE_HOME = "XDG_STATE_HOME";
    static constexpr const char* STATE_PATH_PREFIX = ".local/state";

    static constexpr const char* ENV_HOME = "HOME";

    static constexpr const char* APP_NAME = "clickhouse";

public:
    static std::string getConfigurationHome();
    static std::string getDataHome();
    static std::string getStateHome();
};

}
