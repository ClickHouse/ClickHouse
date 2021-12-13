#include "argsToConfig.h"

#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/MapConfiguration.h>


void argsToConfig(const Poco::Util::Application::ArgVec & argv, Poco::Util::LayeredConfiguration & config, int priority)
{
    /// Parsing all args and converting to config layer
    /// Test: -- --1=1 --1=2 --3 5 7 8 -9 10 -11=12 14= 15== --16==17 --=18 --19= --20 21 22 --23 --24 25 --26 -27 28 ---29=30 -- ----31 32 --33 3-4
    Poco::AutoPtr<Poco::Util::MapConfiguration> map_config = new Poco::Util::MapConfiguration;
    std::string key;
    for (const auto & arg : argv)
    {
        auto key_start = arg.find_first_not_of('-');
        auto pos_minus = arg.find('-');
        auto pos_eq = arg.find('=');

        // old saved '--key', will set to some true value "1"
        if (!key.empty() && pos_minus != std::string::npos && pos_minus < key_start)
        {
            map_config->setString(key, "1");
            key = "";
        }

        if (pos_eq == std::string::npos)
        {
            if (!key.empty())
            {
                if (pos_minus == std::string::npos || pos_minus > key_start)
                {
                    map_config->setString(key, arg);
                }
                key = "";
            }
            if (pos_minus != std::string::npos && key_start != std::string::npos && pos_minus < key_start)
                key = arg.substr(key_start);
            continue;
        }
        else
        {
            key = "";
        }

        if (key_start == std::string::npos)
            continue;

        if (pos_minus > key_start)
            continue;

        key = arg.substr(key_start, pos_eq - key_start);
        if (key.empty())
            continue;
        std::string value;
        if (arg.size() > pos_eq)
            value = arg.substr(pos_eq + 1);

        map_config->setString(key, value);
        key = "";
    }

    Poco::Util::MapConfiguration::Keys keys;
    map_config->keys(keys);

    config.add(map_config, priority);
}
