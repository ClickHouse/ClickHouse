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

SettingsConstraints::SettingsConstraints() = default;
SettingsConstraints::SettingsConstraints(const SettingsConstraints & src) = default;
SettingsConstraints & SettingsConstraints::operator=(const SettingsConstraints & src) = default;
SettingsConstraints::SettingsConstraints(SettingsConstraints && src) = default;
SettingsConstraints & SettingsConstraints::operator=(SettingsConstraints && src) = default;
SettingsConstraints::~SettingsConstraints() = default;


void SettingsConstraints::clear()
{
    constraints_by_index.clear();
}


void SettingsConstraints::setReadOnly(const String & name, bool read_only)
{
    size_t setting_index = Settings::findIndexStrict(name);
    getConstraintRef(setting_index).read_only = read_only;
}

void SettingsConstraints::setMinValue(const String & name, const Field & min_value)
{
    size_t setting_index = Settings::findIndexStrict(name);
    getConstraintRef(setting_index).min_value = Settings::castValueWithoutApplying(setting_index, min_value);
}

void SettingsConstraints::setMaxValue(const String & name, const Field & max_value)
{
    size_t setting_index = Settings::findIndexStrict(name);
    getConstraintRef(setting_index).max_value = Settings::castValueWithoutApplying(setting_index, max_value);
}


void SettingsConstraints::check(const Settings & current_settings, const SettingChange & change) const
{
    const String & name = change.name;
    size_t setting_index = Settings::findIndex(name);
    if (setting_index == Settings::npos)
        return;

    Field new_value = Settings::castValueWithoutApplying(setting_index, change.value);
    Field current_value = current_settings.get(setting_index);

    /// Setting isn't checked if value wasn't changed.
    if (current_value == new_value)
        return;

    if (!current_settings.allow_ddl && name == "allow_ddl")
        throw Exception("Cannot modify 'allow_ddl' setting when DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    /** The `readonly` value is understood as follows:
      * 0 - everything allowed.
      * 1 - only read queries can be made; you can not change the settings.
      * 2 - You can only do read queries and you can change the settings, except for the `readonly` setting.
      */
    if (current_settings.readonly == 1)
        throw Exception("Cannot modify '" + name + "' setting in readonly mode", ErrorCodes::READONLY);

    if (current_settings.readonly > 1 && name == "readonly")
        throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);

    const Constraint * constraint = tryGetConstraint(setting_index);
    if (constraint)
    {
        if (constraint->read_only)
            throw Exception("Setting " + name + " should not be changed", ErrorCodes::SETTING_CONSTRAINT_VIOLATION);

        if (!constraint->min_value.isNull() && (new_value < constraint->min_value))
            throw Exception(
                "Setting " + name + " shouldn't be less than " + applyVisitor(FieldVisitorToString(), constraint->min_value),
                ErrorCodes::SETTING_CONSTRAINT_VIOLATION);

        if (!constraint->max_value.isNull() && (new_value > constraint->max_value))
            throw Exception(
                "Setting " + name + " shouldn't be greater than " + applyVisitor(FieldVisitorToString(), constraint->max_value),
                ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
    }
}


void SettingsConstraints::check(const Settings & current_settings, const SettingsChanges & changes) const
{
    for (const auto & change : changes)
        check(current_settings, change);
}


SettingsConstraints::Constraint & SettingsConstraints::getConstraintRef(size_t index)
{
    auto it = constraints_by_index.find(index);
    if (it == constraints_by_index.end())
        it = constraints_by_index.emplace(index, Constraint{}).first;
    return it->second;
}

const SettingsConstraints::Constraint * SettingsConstraints::tryGetConstraint(size_t index) const
{
    auto it = constraints_by_index.find(index);
    if (it == constraints_by_index.end())
        return nullptr;
    return &it->second;
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
            auto get_constraint_value = [&]{ return config.getString(path_to_name + "." + constraint_type); };
            if (constraint_type == "min")
                setMinValue(name, get_constraint_value());
            else if (constraint_type == "max")
                setMaxValue(name, get_constraint_value());
            else if (constraint_type == "readonly")
                setReadOnly(name, true);
            else
                throw Exception("Setting " + constraint_type + " value for " + name + " isn't supported", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
}

}
