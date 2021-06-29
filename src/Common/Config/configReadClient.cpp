#include "configReadClient.h"

#include <Poco/Util/LayeredConfiguration.h>
#include "ConfigProcessor.h"
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
bool configReadClient(Poco::Util::LayeredConfiguration & config, const std::string & home_path)
{
    auto safe_exists = [](const auto & path)
    {
        std::error_code ec;
        bool res = fs::exists(path, ec);
        if (ec.value() > 0)
        {
            LOG_ERROR(&Poco::Logger::get("ClickHouseClient"), "Can't check '{}': [{}]({})", path, ec.value(), ec.message());
        }
        return res;
    };

    std::string config_path;
    if (config.has("config-file"))
        config_path = config.getString("config-file");
    else if (safe_exists("./clickhouse-client.xml"))
        config_path = "./clickhouse-client.xml";
    else if (!home_path.empty() && safe_exists(home_path + "/.clickhouse-client/config.xml"))
        config_path = home_path + "/.clickhouse-client/config.xml";
    else if (safe_exists("/etc/clickhouse-client/config.xml"))
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
