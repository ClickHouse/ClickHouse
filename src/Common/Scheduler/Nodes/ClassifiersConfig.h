#pragma once

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <unordered_map>

namespace DB
{

/// Mapping of resource name into path string (e.g. "disk1" -> "/path/to/class")
struct ClassifierDescription : std::unordered_map<String, String>
{
    ClassifierDescription() = default;
    ClassifierDescription(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
};

/*
 * Loads a config with the following format:
 * <workload_classifiers>
 *   <classifier1>
 *     <resource1>/path/to/queue</resource1>
 *     <resource2>/path/to/another/queue</resource2>
 *   </classifier1>
 *   ...
 *   <classifierN>...</classifierN>
 * </workload_classifiers>
 */
class ClassifiersConfig
{
public:
    ClassifiersConfig() = default;
    explicit ClassifiersConfig(const Poco::Util::AbstractConfiguration & config);

    const ClassifierDescription & get(const String & classifier_name);

private:
    std::unordered_map<String, ClassifierDescription> classifiers; // by classifier_name
};

}
