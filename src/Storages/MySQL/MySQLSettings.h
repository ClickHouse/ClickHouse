#pragma once

#include <Storages/SettingDescription.h>

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsEnums.h>
#include <Core/SettingsFields.h>
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

    /// A table's own `SETTINGS` clause, recorded as the definition.
    void loadFromQuery(ASTStorage & storage_def);
    /// A clause that is not a table's definition - a `MySQL` database's, the `mysql` table function's - recorded as
    /// nothing, so a database's tables report its values as `other`.
    void loadFromQuery(const ASTSetQuery & settings_def);
    void loadFromQueryContext(ContextPtr context, ASTStorage & storage_def);
    void loadFromNamedCollection(const NamedCollection & named_collection);

    static bool hasBuiltin(std::string_view name);
    DECLARE_SETTINGS_ENUMERATION(MySQLSettings)

private:
    std::unique_ptr<MySQLSettingsImpl> impl;
};


}
