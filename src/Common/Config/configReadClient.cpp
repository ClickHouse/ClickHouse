#include "configReadClient.h"

#include <Poco/Util/LayeredConfiguration.h>
#include "ConfigProcessor.h"
#include <filesystem>
#include <base/types.h>


namespace fs = std::filesystem;

namespace DB
{

bool configReadClient(Poco::Util::LayeredConfiguration & config, const std::string & home_path)
{
    std::string config_path;

    bool found = false;
    if (config.has("config-file"))
    {
        found = true;
        config_path = config.getString("config-file");
    }
    else
    {
        std::vector<std::string> names;
        names.emplace_back("./clickhouse-client");
        if (!home_path.empty())
            names.emplace_back(home_path + "/.clickhouse-client/config");
        names.emplace_back("/etc/clickhouse-client/config");

        for (const auto & name : names)
        {
            for (const auto & extension : {".xml", ".yaml", ".yml"})
            {
                config_path = name + extension;

                std::error_code ec;
                if (fs::exists(config_path, ec))
                {
                    found = true;
                    break;
                }
            }
            if (found)
                break;
        }
    }

    if (found)
    {
        ConfigProcessor config_processor(config_path);
        auto loaded_config = config_processor.loadConfig();
        config.add(loaded_config.configuration);
        return true;
    }

    return false;
}

}
