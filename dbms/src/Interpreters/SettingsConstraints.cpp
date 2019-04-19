#include <Interpreters/SettingsConstraints.h>
#include <Core/Settings.h>
#include <Common/FieldVisitors.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SETTING_CONSTRAINT_VIOLATION;
}

namespace
{
    thread_local Settings temp_settings;
}


SettingsConstraints::SettingsConstraints() = default;
SettingsConstraints::SettingsConstraints(const SettingsConstraints & src) = default;
SettingsConstraints & SettingsConstraints::operator=(const SettingsConstraints & src) = default;
SettingsConstraints::SettingsConstraints(SettingsConstraints && src) = default;
SettingsConstraints & SettingsConstraints::operator=(SettingsConstraints && src) = default;
SettingsConstraints::~SettingsConstraints() = default;


void SettingsConstraints::setMaxValue(const String & name, const String & max_value)
{
    max_settings.set(name, max_value);
}

void SettingsConstraints::setMaxValue(const String & name, const Field & max_value)
{
    max_settings.set(name, max_value);
}


void SettingsConstraints::check(const Settings & current_settings, const SettingChange & change) const
{
    size_t index = current_settings.find(change.name);
    if (index == Settings::npos)
        return;
    // We store `change.value` to `temp_settings` to ensure it's converted to the correct type.
    temp_settings[index].setValue(change.value);
    checkImpl(current_settings, index);
}


void SettingsConstraints::check(const Settings & current_settings, const SettingsChanges & changes) const
{
    for (const auto & change : changes)
        check(current_settings, change);
}


void SettingsConstraints::checkImpl(const Settings & current_settings, size_t index) const
{
    const auto & new_setting = temp_settings[index];
    Field new_value = new_setting.getValue();

    const auto & current_setting = current_settings[index];
    Field current_value = current_setting.getValue();

    /// Setting isn't checked if value wasn't changed.
    if (current_value == new_value)
        return;

    const StringRef & name = new_setting.getName();
    if (!current_settings.allow_ddl && name == "allow_ddl")
        throw Exception("Cannot modify 'allow_ddl' setting when DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    /** The `readonly` value is understood as follows:
      * 0 - everything allowed.
      * 1 - only read queries can be made; you can not change the settings.
      * 2 - You can only do read queries and you can change the settings, except for the `readonly` setting.
      */
    if (current_settings.readonly == 1)
        throw Exception("Cannot modify '" + name.toString() + "' setting in readonly mode", ErrorCodes::READONLY);

    if (current_settings.readonly > 1 && name == "readonly")
        throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);

    const auto & max_setting = max_settings[index];
    if (max_setting.isChanged())
    {
        Field max_value = max_setting.getValue();
        if (new_value > max_value)
            throw Exception(
                "Setting " + name.toString() + " shouldn't be greater than " + applyVisitor(FieldVisitorToString(), max_value),
                ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
    }
}


void SettingsConstraints::setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config)
{
    String parent_profile = "profiles." + profile_name + ".profile";
    if (config.has(parent_profile))
        setProfile(parent_profile, config); // Inheritance of one profile from another.

    String path_to_constraints = "profiles." + profile_name + ".constraints";
    if (config.has(path_to_constraints))
        loadFromConfig(path_to_constraints, config);
}


void SettingsConstraints::loadFromConfig(const String & path_to_constraints, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(path_to_constraints))
        throw Exception("There is no path '" + path_to_constraints + "' in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    Poco::Util::AbstractConfiguration::Keys names;
    config.keys(path_to_constraints, names);

    for (const String & name : names)
    {
        String path_to_name = path_to_constraints + "." + name;
        Poco::Util::AbstractConfiguration::Keys constraint_types;
        config.keys(path_to_name, constraint_types);
        for (const String & constraint_type : constraint_types)
        {
            String path_to_type = path_to_name + "." + constraint_type;
            if (constraint_type == "max")
                setMaxValue(name, config.getString(path_to_type));
            else
                throw Exception("Setting " + constraint_type + " value for " + name + " isn't supported", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
}

}
