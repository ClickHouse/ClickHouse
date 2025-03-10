#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/FormatFactorySettings.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTStorage;
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

    void loadFromQuery(ASTStorage & storage_def);

    Field get(const std::string & name);

private:
    std::unique_ptr<StorageObjectStorageSettingsImpl> impl;
};

}
