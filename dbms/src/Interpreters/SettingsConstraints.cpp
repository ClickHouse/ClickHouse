#include <Interpreters/SettingsConstraints.h>
#include <Core/Settings.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

namespace
{
    thread_local Settings temp_settings;

    void checkImpl(const Settings & current_settings, size_t index)
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
    }
}


void SettingsConstraints::check(const Settings & current_settings, const SettingChange & change)
{
    size_t index = current_settings.find(change.name);
    if (index == Settings::npos)
        return;
    // We store `change.value` to `temp_settings` to ensure it's converted to the correct type.
    temp_settings[index].setValue(change.value);
    checkImpl(current_settings, index);
}


void SettingsConstraints::check(const Settings & current_settings, const SettingsChanges & changes)
{
    for (const auto & change : changes)
        check(current_settings, change);
}

}
