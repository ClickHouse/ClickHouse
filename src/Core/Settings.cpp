#include "Settings.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Common/typeid_cast.h>
#include <string.h>
#include <boost/program_options/options_description.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_PROFILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
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

void Settings::dumpToMapColumn(IColumn * column, bool changed_only)
{
    /// Convert ptr and make simple check
    auto * column_map = column ? &typeid_cast<ColumnMap &>(*column) : nullptr;
    if (!column_map)
        return;

    auto & offsets = column_map->getNestedColumn().getOffsets();
    auto & tuple_column = column_map->getNestedData();
    auto & key_column = tuple_column.getColumn(0);
    auto & value_column = tuple_column.getColumn(1);

    size_t size = 0;
    for (const auto & setting : all(changed_only ? SKIP_UNCHANGED : SKIP_NONE))
    {
        auto name = setting.getName();
        key_column.insertData(name.data(), name.size());
        value_column.insert(setting.getValueString());
        size++;
    }

    offsets.push_back(offsets.back() + size);
}

void Settings::addProgramOptions(boost::program_options::options_description & options)
{
    for (const auto & field : all())
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

void Settings::checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path)
{
    if (config.getBool("skip_check_for_incorrect_settings", false))
        return;

    Settings settings;
    for (auto setting : settings.all())
    {
        const auto & name = setting.getName();
        if (config.has(name))
        {
            throw Exception(fmt::format("A setting '{}' appeared at top level in config {}."
                " But it is user-level setting that should be located in users.xml inside <profiles> section for specific profile."
                " You can add it to <profiles><default> if you want to change default value of this setting."
                " You can also disable the check - specify <skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>"
                " in the main configuration file.",
                name, config_path),
                ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        }
    }
}

IMPLEMENT_SETTINGS_TRAITS(FormatFactorySettingsTraits, FORMAT_FACTORY_SETTINGS)

}
