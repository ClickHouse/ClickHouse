#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/FormatFactorySettings.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTStorage;
struct DatabaseIcebergSettingsImpl;
class SettingsChanges;

/// List of available types supported in DatabaseIcebergSettings object
#define DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, DatabaseIcebergCatalogType) \

DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseIcebergSettings, DECLARE_SETTING_TRAIT)

struct DatabaseIcebergSettings
{
    DatabaseIcebergSettings();
    DatabaseIcebergSettings(const DatabaseIcebergSettings & settings);
    DatabaseIcebergSettings(DatabaseIcebergSettings && settings) noexcept;
    ~DatabaseIcebergSettings();

    DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseIcebergSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(const ASTStorage & storage_def);

    void applyChanges(const SettingsChanges & changes);

private:
    std::unique_ptr<DatabaseIcebergSettingsImpl> impl;
};
}
