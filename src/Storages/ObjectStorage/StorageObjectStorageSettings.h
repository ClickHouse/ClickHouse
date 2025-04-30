#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/FormatFactorySettings.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTSetQuery;
struct StorageObjectStorageSettingsImpl;
struct MutableColumnsAndConstraints;
class StorageObjectStorage;
class SettingsChanges;

/// List of available types supported in StorageObjectStorageSettingsSettings object
#define STORAGE_OBJECT_STORAGE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, ArrowCompression) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, CapnProtoEnumComparingMode) \
    M(CLASS_NAME, Char) \
    M(CLASS_NAME, DateTimeInputFormat) \
    M(CLASS_NAME, DateTimeOutputFormat) \
    M(CLASS_NAME, DateTimeOverflowBehavior) \
    M(CLASS_NAME, Double) \
    M(CLASS_NAME, EscapingRule) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, IdentifierQuotingRule) \
    M(CLASS_NAME, IdentifierQuotingStyle) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, IntervalOutputFormat) \
    M(CLASS_NAME, MsgPackUUIDRepresentation) \
    M(CLASS_NAME, ObjectStorageQueueAction) \
    M(CLASS_NAME, ObjectStorageQueueMode) \
    M(CLASS_NAME, ORCCompression) \
    M(CLASS_NAME, ParquetCompression) \
    M(CLASS_NAME, ParquetVersion) \
    M(CLASS_NAME, SchemaInferenceMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt32) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)

// clang-format off

#define STORAGE_OBJECT_STORAGE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, allow_dynamic_metadata_for_data_lakes, false, R"(
If enabled, indicates that metadata is taken from iceberg specification that is pulled from cloud before each query.
)", 0) \
    DECLARE(Bool, allow_experimental_delta_kernel_rs, false, R"(
If enabled, the engine would use delta-kernel-rs for DeltaLake metadata parsing
)", 0) \
    DECLARE(Bool, delta_lake_read_schema_same_as_table_schema, false, R"(
Whether delta-lake read schema is the same as table schema.
)", 0) \
    DECLARE(String, iceberg_metadata_file_path, "", R"(
Explicit path to desired Iceberg metadata file, should be relative to path in object storage. Make sense for table function use case only.
)", 0) \
    DECLARE(String, iceberg_metadata_table_uuid, "", R"(
Explicit table UUID to read metadata for. Ignored if iceberg_metadata_file_path is set.
)", 0) \
    DECLARE(Bool, iceberg_recent_metadata_file_by_last_updated_ms_field, false, R"(
If enabled, the engine would use the metadata file with the most recent last_updated_ms json field. Does not make sense to use with iceberg_metadata_file_path.
)", 0) \
    DECLARE(Bool, iceberg_use_version_hint, false, R"(
Get latest metadata path from version-hint.text file.
)", 0) \

// clang-format on

STORAGE_OBJECT_STORAGE_SETTINGS_SUPPORTED_TYPES(StorageObjectStorageSettings, DECLARE_SETTING_TRAIT)

struct StorageObjectStorageSettings
{
    StorageObjectStorageSettings();
    StorageObjectStorageSettings(const StorageObjectStorageSettings & settings);
    StorageObjectStorageSettings(StorageObjectStorageSettings && settings) noexcept;
    ~StorageObjectStorageSettings();

    STORAGE_OBJECT_STORAGE_SETTINGS_SUPPORTED_TYPES(StorageObjectStorageSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTSetQuery & settings_ast);

    void loadFromSettingsChanges(const SettingsChanges & changes);

    Field get(const std::string & name);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<StorageObjectStorageSettingsImpl> impl;
};

using StorageObjectStorageSettingsPtr = std::shared_ptr<StorageObjectStorageSettings>;

#define LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS(M, ALIAS) \
    STORAGE_OBJECT_STORAGE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

}
