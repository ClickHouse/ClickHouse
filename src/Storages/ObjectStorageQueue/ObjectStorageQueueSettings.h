#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/FormatFactorySettings.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTStorage;
struct ObjectStorageQueueSettingsImpl;
struct MutableColumnsAndConstraints;
class StorageObjectStorageQueue;
class SettingsChanges;

/// List of available types supported in ObjectStorageQueueSettings object
#define OBJECT_STORAGE_QUEUE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
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
    M(CLASS_NAME, UInt64Auto) \
    M(CLASS_NAME, URI)

OBJECT_STORAGE_QUEUE_SETTINGS_SUPPORTED_TYPES(ObjectStorageQueueSettings, DECLARE_SETTING_TRAIT)

struct ObjectStorageQueueSettings
{
    ObjectStorageQueueSettings();
    ObjectStorageQueueSettings(const ObjectStorageQueueSettings & settings);
    ObjectStorageQueueSettings(ObjectStorageQueueSettings && settings) noexcept;
    ~ObjectStorageQueueSettings();

    OBJECT_STORAGE_QUEUE_SETTINGS_SUPPORTED_TYPES(ObjectStorageQueueSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void dumpToSystemEngineSettingsColumns(
        MutableColumnsAndConstraints & params,
        const std::string & table_name,
        const std::string & database_name,
        const StorageObjectStorageQueue & storage) const;

    void loadFromQuery(ASTStorage & storage_def);

    void applyChanges(const SettingsChanges & changes);

    Field get(const std::string & name);

private:
    std::unique_ptr<ObjectStorageQueueSettingsImpl> impl;
};
}
