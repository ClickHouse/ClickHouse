#include <Common/Config/ConfigHelper.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ConfigHelper
{

bool getBool(const Poco::Util::AbstractConfiguration & config, const std::string & key, bool default_, bool empty_as)
{
    if (!config.has(key))
        return default_;
    Poco::Util::AbstractConfiguration::Keys sub_keys;
    config.keys(key, sub_keys);
    if (sub_keys.size() == 0 && config.getString(key).size() == 0)
        return empty_as;
    return config.getBool(key, default_);
}

}

}
