#pragma once

#include <Storages/TableSetting.h>

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Columns/IColumn_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class ASTStorage;
class SettingsChanges;
struct ExecutableSettingsImpl;

#define EXECUTABLE_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, ExternalCommandStderrReaction) \
    M(CLASS_NAME, UInt64)

EXECUTABLE_SETTINGS_SUPPORTED_TYPES(ExecutableSettings, DECLARE_SETTING_TRAIT)

/// Settings for ExecutablePool engine.
struct ExecutableSettings
{
    std::string script_name;
    VectorWithMemoryTracking<std::string> script_arguments;
    bool is_executable_pool = false;

    ExecutableSettings();
    ExecutableSettings(const ExecutableSettings & settings);
    ExecutableSettings(ExecutableSettings && settings) noexcept;
    ~ExecutableSettings();

    EXECUTABLE_SETTINGS_SUPPORTED_TYPES(ExecutableSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);
    void applyChanges(const SettingsChanges & changes);

    static bool hasBuiltin(std::string_view name);
    /// Every setting of this instance, for `system.table_settings`. The caller refines `origin`.
    TableSettings enumerateSettings() const;
    /// The engine's own settings, for `system.engine_settings`.
    static TableSettings enumerateEngineSettings(ContextPtr context);

private:
    std::unique_ptr<ExecutableSettingsImpl> impl;
};

}
