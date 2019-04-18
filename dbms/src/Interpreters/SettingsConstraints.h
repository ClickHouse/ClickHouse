#pragma once

#include <Common/SettingsChanges.h>

namespace DB
{
struct Settings;

/** Checks if specified changes of settings are allowed or not.
  * If the changes are not allowed (i.e. violates some constraints) this class throws an exception.
  * This class checks that we are not in the read-only mode.
  * If a setting cannot be change due to the read-only mode this class throws an exception.
  * The value of `readonly` value is understood as follows:
  * 0 - everything allowed.
  * 1 - only read queries can be made; you can not change the settings.
  * 2 - you can only do read queries and you can change the settings, except for the `readonly` setting.
  */
class SettingsConstraints
{
public:
    static void check(const Settings & current_settings, const SettingChange & change);
    static void check(const Settings & current_settings, const SettingsChanges & changes);
};

}
