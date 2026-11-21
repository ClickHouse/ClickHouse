#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

#include <memory>

namespace DB
{
class ASTStorage;
struct QueryRunnerSettingsImpl;

/// List of available types supported in QueryRunnerSettings object
#define QUERY_RUNNER_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, QueryRunnerMode) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64)

QUERY_RUNNER_SETTINGS_SUPPORTED_TYPES(QueryRunnerSettings, DECLARE_SETTING_TRAIT)

/** Settings for the QueryRunner engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct QueryRunnerSettings
{
    QueryRunnerSettings();
    QueryRunnerSettings(QueryRunnerSettings && settings) noexcept;
    ~QueryRunnerSettings();

    QUERY_RUNNER_SETTINGS_SUPPORTED_TYPES(QueryRunnerSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<QueryRunnerSettingsImpl> impl;
};

}
