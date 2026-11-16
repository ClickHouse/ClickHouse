#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
class ASTStorage;
class Context;
struct MergeTreeSettings;
struct DatabaseMetadataDiskSettingsImpl;

/// List of available types supported in MetadataDiskSettings object
#define DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, String)

DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(DatabaseMetadataDiskSettings, DECLARE_SETTING_TRAIT)

struct DatabaseMetadataDiskSettings
{
    DatabaseMetadataDiskSettings();
    DatabaseMetadataDiskSettings(const DatabaseMetadataDiskSettings & settings);
    DatabaseMetadataDiskSettings(DatabaseMetadataDiskSettings && settings) noexcept;
    ~DatabaseMetadataDiskSettings();

    DATABASE_METADATA_SETTINGS_SUPPORTED_TYPES(DatabaseMetadataDiskSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    /// `stored_definition`: the definition was read from the metadata the server stored for the database (short `ATTACH`).
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_loading_from_existing_metadata, bool stored_definition = false);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<DatabaseMetadataDiskSettingsImpl> impl;
};

}
