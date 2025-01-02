#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

namespace DB
{

class ASTStorage;
struct MaterializedMySQLSettingsImpl;

/// List of available types supported in MaterializedMySQLSettings object
#define MATERIALIZED_MYSQL_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Int64) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, String)

MATERIALIZED_MYSQL_SETTINGS_SUPPORTED_TYPES(MaterializedMySQLSettings, DECLARE_SETTING_TRAIT)

/** Settings for the MaterializedMySQL database engine.
  * Could be loaded from a CREATE DATABASE query (SETTINGS clause).
  */
struct MaterializedMySQLSettings
{
    MaterializedMySQLSettings();
    MaterializedMySQLSettings(const MaterializedMySQLSettings & settings);
    MaterializedMySQLSettings(MaterializedMySQLSettings && settings) noexcept;
    ~MaterializedMySQLSettings();

    MATERIALIZED_MYSQL_SETTINGS_SUPPORTED_TYPES(MaterializedMySQLSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def);

private:
    std::unique_ptr<MaterializedMySQLSettingsImpl> impl;
};

}
