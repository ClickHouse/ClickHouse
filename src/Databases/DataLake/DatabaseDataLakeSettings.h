#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTStorage;
struct DatabaseDataLakeSettingsImpl;
class SettingsChanges;

/// List of available types supported in DatabaseDataLakeSettings object
#define DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, DatabaseDataLakeCatalogType) \

#define LIST_OF_DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    STORAGE_OBJECT_STORAGE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M)

LIST_OF_DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseDataLakeSettings, DECLARE_SETTING_TRAIT)

struct DatabaseDataLakeSettings
{
    DatabaseDataLakeSettings();
    DatabaseDataLakeSettings(const DatabaseDataLakeSettings & settings);
    DatabaseDataLakeSettings(DatabaseDataLakeSettings && settings) noexcept;
    ~DatabaseDataLakeSettings();

    DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(DatabaseDataLakeSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(const ASTStorage & storage_def, bool is_attach);

    void applyChanges(const SettingsChanges & changes);

    SettingsChanges allChanged() const;

private:
    std::unique_ptr<DatabaseDataLakeSettingsImpl> impl;
};
}
