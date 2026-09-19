#pragma once

#include <Storages/SettingDescription.h>

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Interpreters/Context_fwd.h>

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

    /// The table's own `SETTINGS` clause, recorded as the definition.
    void loadFromQuery(const ASTStorage & storage_def);
    /// The same for the whole clause as `ALTER ... MODIFY` or `RESET SETTING` leaves it.
    void applyDefinition(const SettingsChanges & changes);

    static bool hasBuiltin(std::string_view name);
    DECLARE_SETTINGS_ENUMERATION(RocksDBSettings)
    static void checkCanSet(std::string_view name, const Field & value);

private:
    std::unique_ptr<RocksDBSettingsImpl> impl;
};
}
