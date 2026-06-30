#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{
class ASTStorage;
class Context;
struct MergeTreeSettings;
struct DatabaseMetadataDiskSettingsImpl;

/// List of available types supported in MetadataDiskSettings object
#define DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, String)

DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(DatabaseMetadataDiskSettings, DECLARE_SETTING_TRAIT)

struct DatabaseMetadataDiskSettings
{
    DatabaseMetadataDiskSettings();
    DatabaseMetadataDiskSettings(const DatabaseMetadataDiskSettings & settings);
    DatabaseMetadataDiskSettings(DatabaseMetadataDiskSettings && settings) noexcept;
    ~DatabaseMetadataDiskSettings();

    DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(DatabaseMetadataDiskSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach);

private:
    std::unique_ptr<DatabaseMetadataDiskSettingsImpl> impl;
};

}
