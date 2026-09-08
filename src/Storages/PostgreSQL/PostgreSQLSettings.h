#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{
class ASTStorage;
class ASTSetQuery;
class Context;
class NamedCollection;
struct PostgreSQLSettingsImpl;

/// List of available types supported in PostgreSQLSettings object
#define POSTGRESQL_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64)

POSTGRESQL_SETTINGS_SUPPORTED_TYPES(PostgreSQLSettings, DECLARE_SETTING_TRAIT)


/** Settings for the PostgreSQL table engine and the `postgresql` table function.
  * The defaults mirror the corresponding query-level `postgresql_*` settings so that a table created
  * without a SETTINGS clause behaves exactly as before.
  */
struct PostgreSQLSettings
{
    PostgreSQLSettings();
    PostgreSQLSettings(const PostgreSQLSettings & settings);
    PostgreSQLSettings(PostgreSQLSettings && settings) noexcept;
    ~PostgreSQLSettings();

    POSTGRESQL_SETTINGS_SUPPORTED_TYPES(PostgreSQLSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    VectorWithMemoryTracking<std::string_view> getAllRegisteredNames() const;

    void loadFromQuery(const ASTSetQuery & settings_def);
    void loadFromQuery(ASTStorage & storage_def);
    void loadFromNamedCollection(const NamedCollection & named_collection);

    /// Seed the connection-pool settings from the query-level `postgresql_*` settings. This keeps the
    /// historical behaviour where the pool parameters were taken from the query context; an explicit
    /// SETTINGS clause (applied afterwards) overrides them.
    void loadFromQueryContext(const Context & context);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<PostgreSQLSettingsImpl> impl;
};

}
