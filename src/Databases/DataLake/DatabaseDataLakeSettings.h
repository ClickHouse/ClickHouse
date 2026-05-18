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
    M(CLASS_NAME, S3UriStyle) \

/// Merged and deduplicated type list for DatabaseDataLake + StorageObjectStorage settings.
#define LIST_OF_DATABASE_ICEBERG_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ArrowCompression) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CapnProtoEnumComparingMode) \
    M(CLASS_NAME, Char) \
    M(CLASS_NAME, DatabaseDataLakeCatalogType) \
    M(CLASS_NAME, DateTimeInputFormat) \
    M(CLASS_NAME, DateTimeOutputFormat) \
    M(CLASS_NAME, DateTimeOverflowBehavior) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, EscapingRule) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, IdentifierQuotingRule) \
    M(CLASS_NAME, IdentifierQuotingStyle) \
    M(CLASS_NAME, InputFormatColumnMatchingCaseSensitivity) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, IntervalOutputFormat) \
    M(CLASS_NAME, Map) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, ObjectStorageQueueAction) \
    M(CLASS_NAME, ObjectStorageQueueMode) \
    M(CLASS_NAME, ORCCompression) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, S3UriStyle) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt32) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)

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
