#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
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

private:
    std::unique_ptr<ExecutableSettingsImpl> impl;
};

}
