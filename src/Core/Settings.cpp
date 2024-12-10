#include "Settings.h"

#include <Core/SettingsChangesHistory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Common/typeid_cast.h>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int THERE_IS_NO_PROFILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTINGS_TRAITS(SettingsTraits, LIST_OF_SETTINGS)

/** Set the settings from the profile (in the server configuration, many settings can be listed in one profile).
    * The profile can also be set using the `set` functions, like the `profile` setting.
    */
void Settings::setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config)
{
    String elem = "profiles." + profile_name;

    if (!config.has(elem))
        throw Exception(ErrorCodes::THERE_IS_NO_PROFILE, "There is no profile '{}' in configuration file.", profile_name);

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
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no path '{}' in configuration file.", path);

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

void Settings::checkNoSettingNamesAtTopLevel(const Poco::Util::AbstractConfiguration & config, const String & config_path)
{
    if (config.getBool("skip_check_for_incorrect_settings", false))
        return;

    Settings settings;
    for (const auto & setting : settings.all())
    {
        const auto & name = setting.getName();
        bool should_skip_check = name == "max_table_size_to_drop" || name == "max_partition_size_to_drop";
        if (config.has(name) && !setting.isObsolete() && !should_skip_check)
        {
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "A setting '{}' appeared at top level in config {}."
                " But it is user-level setting that should be located in users.xml inside <profiles> section for specific profile."
                " You can add it to <profiles><default> if you want to change default value of this setting."
                " You can also disable the check - specify <skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>"
                " in the main configuration file.",
                name, config_path);
        }
    }
}

std::vector<String> Settings::getAllRegisteredNames() const
{
    std::vector<String> all_settings;
    for (const auto & setting_field : all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
}

void Settings::set(std::string_view name, const Field & value)
{
    if (name == "compatibility")
    {
        if (value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type of value for setting 'compatibility'. Expected String, got {}", value.getTypeName());
        applyCompatibilitySetting(value.get<String>());
    }
    /// If we change setting that was changed by compatibility setting before
    /// we should remove it from settings_changed_by_compatibility_setting,
    /// otherwise the next time we will change compatibility setting
    /// this setting will be changed too (and we don't want it).
    else if (settings_changed_by_compatibility_setting.contains(name))
        settings_changed_by_compatibility_setting.erase(name);

    BaseSettings::set(name, value);
}

void Settings::applyCompatibilitySetting(const String & compatibility_value)
{
    /// First, revert all changes applied by previous compatibility setting
    for (const auto & setting_name : settings_changed_by_compatibility_setting)
        resetToDefault(setting_name);

    settings_changed_by_compatibility_setting.clear();
    /// If setting value is empty, we don't need to change settings
    if (compatibility_value.empty())
        return;

    ClickHouseVersion version(compatibility_value);
    /// Iterate through ClickHouse version in descending order and apply reversed
    /// changes for each version that is higher that version from compatibility setting
    for (auto it = settings_changes_history.rbegin(); it != settings_changes_history.rend(); ++it)
    {
        if (version >= it->first)
            break;

        /// Apply reversed changes from this version.
        for (const auto & change : it->second)
        {
            /// If this setting was changed manually, we don't change it
            if (isChanged(change.name) && !settings_changed_by_compatibility_setting.contains(change.name))
                continue;

            BaseSettings::set(change.name, change.previous_value);
            settings_changed_by_compatibility_setting.insert(change.name);
        }
    }
}

IMPLEMENT_SETTINGS_TRAITS(FormatFactorySettingsTraits, LIST_OF_ALL_FORMAT_SETTINGS)

}
