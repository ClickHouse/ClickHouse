#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>


namespace DB
{
class ASTStorage;
struct SettingChange;
struct MaterializedPostgreSQLSettingsImpl;

#define MATERIALIZED_POSTGRESQL_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64)

MATERIALIZED_POSTGRESQL_SETTINGS_SUPPORTED_TYPES(MaterializedPostgreSQLSettings, DECLARE_SETTING_TRAIT)


struct MaterializedPostgreSQLSettings
{
    MaterializedPostgreSQLSettings();
    MaterializedPostgreSQLSettings(const MaterializedPostgreSQLSettings & settings);
    MaterializedPostgreSQLSettings(MaterializedPostgreSQLSettings && settings) noexcept;
    ~MaterializedPostgreSQLSettings();

    MATERIALIZED_POSTGRESQL_SETTINGS_SUPPORTED_TYPES(MaterializedPostgreSQLSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void applyChange(const SettingChange & change);
    bool has(std::string_view name) const;
    void loadFromQuery(ASTStorage & storage_def);

private:
    std::unique_ptr<MaterializedPostgreSQLSettingsImpl> impl;
};

}

#endif
