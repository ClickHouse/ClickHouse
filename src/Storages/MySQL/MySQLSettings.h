#pragma once

#include <Storages/SettingDescription.h>

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
#include <Interpreters/Context_fwd.h>
#include <Common/VectorWithMemoryTracking.h>

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
struct MySQLSettingsImpl;

/// List of available types supported in MySQLSettings object
#define MYSQL_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, MySQLDataTypesSupport)

MYSQL_SETTINGS_SUPPORTED_TYPES(MySQLSettings, DECLARE_SETTING_TRAIT)


/** Settings for the MySQL family of engines.
  */
struct MySQLSettings
{
    MySQLSettings();
    MySQLSettings(const MySQLSettings & settings);
    MySQLSettings(MySQLSettings && settings) noexcept;
    ~MySQLSettings();

    MYSQL_SETTINGS_SUPPORTED_TYPES(MySQLSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    VectorWithMemoryTracking<std::string_view> getAllRegisteredNames() const;

    /// Records the clause as the source of what it assigns: `Definition` for a table's own `SETTINGS` clause,
    /// none for a `MySQL` database's, which its tables report as `other`, not as their definition.
    void loadFromQuery(ASTStorage & storage_def, SettingOrigin origin = SettingOrigin::Definition);
    void loadFromQuery(const ASTSetQuery & settings_def);
    void loadFromQueryContext(ContextPtr context, ASTStorage & storage_def);
    void loadFromNamedCollection(const NamedCollection & named_collection);

    static bool hasBuiltin(std::string_view name);
    DECLARE_SETTINGS_ENUMERATION(MySQLSettings)

private:
    std::unique_ptr<MySQLSettingsImpl> impl;
};


}
