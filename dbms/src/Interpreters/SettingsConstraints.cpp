#include <Interpreters/SettingsConstraints.h>

#include <Core/Settings.h>
#include <Core/SettingsChanges.h>
#include <Common/FieldVisitors.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
}


void SettingsConstraints::check(const Settings & settings, const SettingChange & change)
{
    Field current_value;
    if (!settings.tryGet(change.name, current_value))
        return;

    /// Setting isn't checked if value wasn't changed.
    if (current_value == change.value)
        return;

    if (!settings.allow_ddl && change.name == "allow_ddl")
        throw Exception("Cannot modify 'allow_ddl' setting when DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    /** The `readonly` value is understood as follows:
      * 0 - everything allowed.
      * 1 - only read queries can be made; you can not change the settings.
      * 2 - You can only do read queries and you can change the settings, except for the `readonly` setting.
      */
    if (settings.readonly == 1)
        throw Exception("Cannot execute SET query in readonly mode", ErrorCodes::READONLY);

    if (settings.readonly > 1 && change.name == "readonly")
        throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);
}


void SettingsConstraints::check(const Settings & settings, const SettingsChanges & changes)
{
    for (const SettingChange & change : changes)
        check(settings, change);
}

}
