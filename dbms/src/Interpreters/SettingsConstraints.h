#pragma once


namespace DB
{
class SettingsChanges;
struct SettingChange;
struct Settings;


class SettingsConstraints
{
public:
    static void check(const Settings & settings, const SettingChange & change);
    static void check(const Settings & settings, const SettingsChanges & changes);
};

}
