#pragma once

#include <Poco/DOM/Document.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Core/Types.h>
#include <vector>
#include <string>

namespace DB
{

using XMLConfiguration = Poco::Util::XMLConfiguration;
using XMLConfigurationPtr = Poco::AutoPtr<XMLConfiguration>;
using XMLDocumentPtr = Poco::AutoPtr<Poco::XML::Document>;

class ConfigPreprocessor
{
public:
    ConfigPreprocessor(const Strings & paths_)
        : paths(paths_)
    {}

    std::vector<XMLConfigurationPtr> processConfig(
        const Strings & tests_tags,
        const Strings & tests_names,
        const Strings & tests_names_regexp,
        const Strings & skip_tags,
        const Strings & skip_names,
        const Strings & skip_names_regexp) const;

private:

    enum class FilterType
    {
        Tag,
        Name,
        Name_regexp
    };

    /// Removes configurations that has a given value.
    /// If leave is true, the logic is reversed.
    void removeConfigurationsIf(
        std::vector<XMLConfigurationPtr> & configs,
        FilterType filter_type,
        const Strings & values,
        bool leave = false) const;

    const Strings paths;
};
}
