#include <Common/Scheduler/Nodes/ClassifiersConfig.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
}

ClassifierDescription::ClassifierDescription(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
        emplace(key, config.getString(config_prefix + "." + key));
}

ClassifiersConfig::ClassifiersConfig(const Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    const String config_prefix = "workload_classifiers";
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
        classifiers.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple(config, config_prefix + "." + key));
}

const ClassifierDescription & ClassifiersConfig::get(const String & classifier_name)
{
    if (auto it = classifiers.find(classifier_name); it != classifiers.end())
        return it->second;
    throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Unknown workload classifier '{}' to access resources", classifier_name);
}

}
