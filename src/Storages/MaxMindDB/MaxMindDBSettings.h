#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

#include <memory>

namespace DB
{
class ASTStorage;
struct MaxMindDBSettingsImpl;

#define MAXMINDDB_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64)

MAXMINDDB_SETTINGS_SUPPORTED_TYPES(MaxMindDBSettings, DECLARE_SETTING_TRAIT)

struct MaxMindDBSettings
{
    MaxMindDBSettings();
    ~MaxMindDBSettings();

    MAXMINDDB_SETTINGS_SUPPORTED_TYPES(MaxMindDBSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    static bool hasBuiltin(std::string_view name);
    UInt64 refreshIntervalMilliseconds() const;

private:
    std::unique_ptr<MaxMindDBSettingsImpl> impl;
};
}
