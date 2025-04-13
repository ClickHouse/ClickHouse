#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;
class ASTSetQuery;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class NamedCollection;
struct PostgreSQLSettingsImpl;

/// List of available types supported in PostgreSQLSettings object
#define POSTGRESQL_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64)

POSTGRESQL_SETTINGS_SUPPORTED_TYPES(PostgreSQLSettings, DECLARE_SETTING_TRAIT)


/** Settings for the PostgreSQL family of engines.
  */
struct PostgreSQLSettings
{
    PostgreSQLSettings();
    PostgreSQLSettings(const PostgreSQLSettings & settings);
    PostgreSQLSettings(PostgreSQLSettings && settings) noexcept;
    ~PostgreSQLSettings();

    POSTGRESQL_SETTINGS_SUPPORTED_TYPES(PostgreSQLSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    std::vector<std::string_view> getAllRegisteredNames() const;

    void loadFromQuery(ASTStorage & storage_def);
    void loadFromQuery(const ASTSetQuery & settings_def);
    void loadFromNamedCollection(const NamedCollection & named_collection);

    static bool hasBuiltin(std::string_view name);

private:
    std::unique_ptr<PostgreSQLSettingsImpl> impl;
};


}
