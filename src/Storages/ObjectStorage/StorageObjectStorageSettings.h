#pragma once

#include <Core/BaseSettingsFwdMacros.h>
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
    M(CLASS_NAME, GeoJSONUnsupportedGeometryHandling) \
    M(CLASS_NAME, IdentifierQuotingRule) \
    M(CLASS_NAME, IdentifierQuotingStyle) \
    M(CLASS_NAME, InputFormatColumnMatchingCaseSensitivity) \
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

    /// Whether `name` is a setting of the engine itself (e.g. `object_storage_cluster`), not a format setting.
    /// Such a setting is not a query setting, so it must not be applied to `Settings`.
    static bool isEngineOnlySetting(std::string_view name);

private:
    std::unique_ptr<StorageObjectStorageSettingsImpl> impl;
};

using StorageObjectStorageSettingsPtr = std::shared_ptr<StorageObjectStorageSettings>;

/// The settings of the engine itself, as opposed to the format settings below: these are not query
/// settings, so they must not be applied to the query `Settings` (see `isEngineOnlySetting`).
#define STORAGE_OBJECT_STORAGE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, object_storage_cluster, "", R"(
The name of a cluster to read the table through: a `SELECT` lists the files on the initiator and hands them out to the nodes of the cluster, like the `*Cluster` table functions (`s3Cluster`, `azureBlobStorageCluster`). The cluster is looked up at query time only. An empty string means the table is read on the current node. `INSERT` is not affected. See also the `use_object_storage_cluster` setting.
)", 0) \

#define LIST_OF_STORAGE_OBJECT_STORAGE_SETTINGS(M, ALIAS) \
    STORAGE_OBJECT_STORAGE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

}
