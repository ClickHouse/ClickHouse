#include "Settings.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <string.h>
#include <boost/program_options/options_description.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_PROFILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
}


IMPLEMENT_SETTINGS_TRAITS(SettingsTraits, LIST_OF_SETTINGS)


/** Set the settings from the profile (in the server configuration, many settings can be listed in one profile).
    * The profile can also be set using the `set` functions, like the `profile` setting.
    */
void Settings::setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config)
{
    String elem = "profiles." + profile_name;

    if (!config.has(elem))
        throw Exception("There is no profile '" + profile_name + "' in configuration file.", ErrorCodes::THERE_IS_NO_PROFILE);

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(elem, config_keys);

    for (const std::string & key : config_keys)
    {
        if (key == "constraints")
            continue;
        if (key == "profile" || key.starts_with("profile["))   /// Inheritance of profiles from the current one.
            setProfile(config.getString(elem + "." + key), config);
        else
            set(key, config.getString(elem + "." + key));
    }
}

void Settings::loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(path))
        throw Exception("There is no path '" + path + "' in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(path, config_keys);

    for (const std::string & key : config_keys)
    {
        set(key, config.getString(path + "." + key));
    }
}

void Settings::dumpToArrayColumns(IColumn * column_names_, IColumn * column_values_, bool changed_only)
{
    /// Convert ptr and make simple check
    auto * column_names = (column_names_) ? &typeid_cast<ColumnArray &>(*column_names_) : nullptr;
    auto * column_values = (column_values_) ? &typeid_cast<ColumnArray &>(*column_values_) : nullptr;

    size_t count = 0;

    for (auto setting : all(changed_only ? SKIP_UNCHANGED : SKIP_NONE))
    {
        if (column_names)
        {
            auto name = setting.getName();
            column_names->getData().insertData(name.data(), name.size());
        }
        if (column_values)
            column_values->getData().insert(setting.getValueString());
        ++count;
    }

    if (column_names)
    {
        auto & offsets = column_names->getOffsets();
        offsets.push_back(offsets.back() + count);
    }

    /// Nested columns case
    bool the_same_offsets = column_names && column_values && column_names->getOffsetsPtr() == column_values->getOffsetsPtr();

    if (column_values && !the_same_offsets)
    {
        auto & offsets = column_values->getOffsets();
        offsets.push_back(offsets.back() + count);
    }
}

void Settings::addProgramOptions(boost::program_options::options_description & options)
{
    for (auto field : all())
    {
        const std::string_view name = field.getName();
        auto on_program_option
            = boost::function1<void, const std::string &>([this, name](const std::string & value) { set(name, value); });
        options.add(boost::shared_ptr<boost::program_options::option_description>(new boost::program_options::option_description(
            name.data(),
            boost::program_options::value<std::string>()->composing()->notifier(on_program_option),
            field.getDescription())));
    }
}
}
