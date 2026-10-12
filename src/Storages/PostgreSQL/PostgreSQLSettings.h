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

    /// Take the connection-pool settings that are not set yet - by a named collection or a SETTINGS clause - from the
    /// query-level `postgresql_*` settings of `context`. This keeps the historical behaviour where the pool parameters
    /// were taken from the query context, and a named collection or an explicit SETTINGS clause overrides them.
    void loadFromQueryContext(const Context & context);

    /// The same for a new table, which also adds to the SETTINGS of `storage_def` each value taken from `context` that
    /// differs from the one `attach_context` gives. When the table is attached again, e.g. on server startup, it is
    /// attached in a context with other settings, and keeps these values only from its definition.
    void loadFromQueryContext(const Context & context, const Context & attach_context, ASTStorage & storage_def);

    static bool hasBuiltin(std::string_view name);

private:
    void loadFromQueryContextImpl(const Context & context, const Context * attach_context, ASTStorage * storage_def);

    std::unique_ptr<PostgreSQLSettingsImpl> impl;
};

}
