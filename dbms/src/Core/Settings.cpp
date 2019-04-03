#include "Settings.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/Field.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <string.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int THERE_IS_NO_PROFILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
}


/// Set the configuration by name.
void Settings::set(const String & name, const Field & value)
{
#define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) NAME.set(value);

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_SET)
    else
        throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

/** Set the setting by name. Read the value in text form from a string (for example, from a config, or from a URL parameter).
    */
void Settings::set(const String & name, const String & value)
{
#define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) NAME.set(value);

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_SET)
    else
        throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

String Settings::get(const String & name) const
{
#define GET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) return NAME.toString();

    if (false) {}
    APPLY_FOR_SETTINGS(GET)
    else
        throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef GET
}

bool Settings::tryGet(const String & name, String & value) const
{
#define TRY_GET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) { value = NAME.toString(); return true; }

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_GET)
    else
        return false;

#undef TRY_GET
}

bool Settings::tryGet(const String & name, Field & value) const
{
#define TRY_GET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) { value = NAME.toField(); return true; }

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_GET)
    else
        return false;

#undef TRY_GET
}

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
        if (key == "profile")   /// Inheritance of one profile from another.
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

SettingsChanges Settings::changes() const
{
    SettingsChanges changes;
#define COLLECT_CHANGES(TYPE, NAME, DEFAULT, DESCRIPTION) \
    if (NAME.changed) \
        changes.push_back(#NAME, NAME.toField());

    APPLY_FOR_SETTINGS(COLLECT_CHANGES)
#undef COLLECT_CHANGES
    return changes;
}

void Settings::applyChange(const SettingChange & change)
{
    set(change.name, change.value);
}

void Settings::applyChanges(const SettingsChanges & changes)
{
    for (const auto & change : changes)
        applyChange(change);
}

void Settings::dumpToArrayColumns(IColumn * column_names_, IColumn * column_values_, bool changed_only)
{
    /// Convert ptr and make simple check
    auto column_names = (column_names_) ? &typeid_cast<ColumnArray &>(*column_names_) : nullptr;
    auto column_values = (column_values_) ? &typeid_cast<ColumnArray &>(*column_values_) : nullptr;

    size_t size = 0;

#define ADD_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION)               \
    if (!changed_only || NAME.changed)                              \
    {                                                               \
        if (column_names)                                           \
            column_names->getData().insertData(#NAME, strlen(#NAME)); \
        if (column_values)                                          \
            column_values->getData().insert(NAME.toString());       \
        ++size;                                                     \
    }
    APPLY_FOR_SETTINGS(ADD_SETTING)
#undef ADD_SETTING

    if (column_names)
    {
        auto & offsets = column_names->getOffsets();
        offsets.push_back(offsets.back() + size);
    }

    /// Nested columns case
    bool the_same_offsets = column_names && column_values && column_names->getOffsetsPtr() == column_values->getOffsetsPtr();

    if (column_values && !the_same_offsets)
    {
        auto & offsets = column_values->getOffsets();
        offsets.push_back(offsets.back() + size);
    }
}

}
