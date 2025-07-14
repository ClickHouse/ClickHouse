#include <Common/Config/ConfigHelper.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>


namespace DB
{

namespace ConfigHelper
{

namespace
{
    void cloneImpl(Poco::Util::AbstractConfiguration & dest, const Poco::Util::AbstractConfiguration & src, const std::string & prefix = "")
    {
        std::vector<std::string> keys;
        src.keys(prefix, keys);
        if (!keys.empty())
        {
            std::string prefix_with_dot = prefix + ".";
            for (const auto & key : keys)
                cloneImpl(dest, src, prefix_with_dot + key);
        }
        else if (!prefix.empty())
        {
            dest.setString(prefix, src.getRawString(prefix));
        }
    }
}


Poco::AutoPtr<Poco::Util::AbstractConfiguration> clone(const Poco::Util::AbstractConfiguration & src)
{
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> res(new Poco::Util::XMLConfiguration());
    cloneImpl(*res, src);
    return res;
}

bool getBool(const Poco::Util::AbstractConfiguration & config, const std::string & key, bool default_, bool empty_as)
{
    if (!config.has(key))
        return default_;
    Poco::Util::AbstractConfiguration::Keys sub_keys;
    config.keys(key, sub_keys);
    if (sub_keys.empty() && config.getString(key).empty())
        return empty_as;
    return config.getBool(key, default_);
}

}

}
