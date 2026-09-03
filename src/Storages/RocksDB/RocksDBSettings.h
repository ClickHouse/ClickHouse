#pragma once

#include <Storages/TableSetting.h>

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
struct MutableColumnsAndConstraints;
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

    static bool hasBuiltin(std::string_view name);
    /// Every setting of this instance, for `system.table_settings`. The caller refines `origin`.
    TableSettings enumerateSettings() const;
    static void fillEngineSettingsColumns(MutableColumnsAndConstraints & params, ContextPtr context);
    static void checkCanSet(std::string_view name, const Field & value);

private:
    std::unique_ptr<RocksDBSettingsImpl> impl;
};
}
