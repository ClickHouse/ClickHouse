#include "configReadClient.h"

#include <Poco/Util/LayeredConfiguration.h>
#include "ConfigProcessor.h"
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

namespace DB
{

/// Checks if file exists without throwing an exception but with message in console.
bool safeFsExists(const auto & path)
{
    std::error_code ec;
    bool res = fs::exists(path, ec);
    if (ec)
    {
        std::cerr << "Can't check '" << path << "': [" << ec.value() << "] "  << ec.message() << std::endl;
    }
    return res;
};

bool configReadClient(Poco::Util::LayeredConfiguration & config, const std::string & home_path)
{
    std::string config_path;
    if (config.has("config-file"))
        config_path = config.getString("config-file");
    else if (safeFsExists("./clickhouse-client.xml"))
        config_path = "./clickhouse-client.xml";
    else if (!home_path.empty() && safeFsExists(home_path + "/.clickhouse-client/config.xml"))
        config_path = home_path + "/.clickhouse-client/config.xml";
    else if (safeFsExists("/etc/clickhouse-client/config.xml"))
        config_path = "/etc/clickhouse-client/config.xml";

    if (!config_path.empty())
    {
        ConfigProcessor config_processor(config_path);
        auto loaded_config = config_processor.loadConfig();
        config.add(loaded_config.configuration);
        return true;
    }
    return false;
}
}
