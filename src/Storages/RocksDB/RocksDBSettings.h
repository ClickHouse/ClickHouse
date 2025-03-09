#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace DB
{
class ASTStorage;
struct RocksDBSettingsImpl;
class SettingsChanges;

/// List of available types supported in RocksDBSettings object
#define ROCKSDB_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64)

ROCKSDB_SETTINGS_SUPPORTED_TYPES(RocksDBSettings, DECLARE_SETTING_TRAIT)

struct RocksDBSettings
{
    RocksDBSettings();
    RocksDBSettings(const RocksDBSettings & settings);
    RocksDBSettings(RocksDBSettings && settings) noexcept;
    ~RocksDBSettings();

    ROCKSDB_SETTINGS_SUPPORTED_TYPES(RocksDBSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void applyChanges(const SettingsChanges & changes);
    void loadFromQuery(const ASTStorage & storage_def);

private:
    std::unique_ptr<RocksDBSettingsImpl> impl;
};
}
