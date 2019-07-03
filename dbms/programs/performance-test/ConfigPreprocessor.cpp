#include "ConfigPreprocessor.h"
#include <Core/Types.h>
#include <Poco/Path.h>
#include <regex>
namespace DB
{
std::vector<XMLConfigurationPtr> ConfigPreprocessor::processConfig(
    const Strings & tests_tags,
    const Strings & tests_names,
    const Strings & tests_names_regexp,
    const Strings & skip_tags,
    const Strings & skip_names,
    const Strings & skip_names_regexp) const
{

    std::vector<XMLConfigurationPtr> result;
    for (const auto & path : paths)
    {
        result.emplace_back(XMLConfigurationPtr(new XMLConfiguration(path)));
        result.back()->setString("path", Poco::Path(path).absolute().toString());
    }

    /// Leave tests:
    removeConfigurationsIf(result, FilterType::Tag, tests_tags, true);
    removeConfigurationsIf(result, FilterType::Name, tests_names, true);
    removeConfigurationsIf(result, FilterType::Name_regexp, tests_names_regexp, true);

    /// Skip tests
    removeConfigurationsIf(result, FilterType::Tag, skip_tags, false);
    removeConfigurationsIf(result, FilterType::Name, skip_names, false);
    removeConfigurationsIf(result, FilterType::Name_regexp, skip_names_regexp, false);
    return result;
}

void ConfigPreprocessor::removeConfigurationsIf(
    std::vector<XMLConfigurationPtr> & configs,
    ConfigPreprocessor::FilterType filter_type,
    const Strings & values,
    bool leave) const
{
    auto checker = [&filter_type, &values, &leave] (XMLConfigurationPtr & config)
    {
        if (values.size() == 0)
            return false;

        bool remove_or_not = false;

        if (filter_type == FilterType::Tag)
        {
            Strings tags_keys;
            config->keys("tags", tags_keys);

            Strings tags(tags_keys.size());
            for (size_t i = 0; i != tags_keys.size(); ++i)
                tags[i] = config->getString("tags.tag[" + std::to_string(i) + "]");

            for (const std::string & config_tag : tags)
            {
                if (std::find(values.begin(), values.end(), config_tag) != values.end())
                    remove_or_not = true;
            }
        }

        if (filter_type == FilterType::Name)
        {
            remove_or_not = (std::find(values.begin(), values.end(), config->getString("name", "")) != values.end());
        }

        if (filter_type == FilterType::Name_regexp)
        {
            std::string config_name = config->getString("name", "");
            auto regex_checker = [&config_name](const std::string & name_regexp)
            {
                std::regex pattern(name_regexp);
                return std::regex_search(config_name, pattern);
            };

            remove_or_not = config->has("name") ? (std::find_if(values.begin(), values.end(), regex_checker) != values.end()) : false;
        }

        if (leave)
            remove_or_not = !remove_or_not;
        return remove_or_not;
    };

    auto new_end = std::remove_if(configs.begin(), configs.end(), checker);
    configs.erase(new_end, configs.end());
}

}
