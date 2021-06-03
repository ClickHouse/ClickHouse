#include "configReadClient.h"

#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/File.h>
#include "ConfigProcessor.h"

namespace DB
{
bool configReadClient(Poco::Util::LayeredConfiguration & config, const std::string & home_path)
{
    std::string config_path;
    if (config.has("config-file"))
        config_path = config.getString("config-file");
    else if (Poco::File("./clickhouse-client.xml").exists())
        config_path = "./clickhouse-client.xml";
    else if (!home_path.empty() && Poco::File(home_path + "/.clickhouse-client/config.xml").exists())
        config_path = home_path + "/.clickhouse-client/config.xml";
    else if (Poco::File("/etc/clickhouse-client/config.xml").exists())
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
