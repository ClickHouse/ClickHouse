#pragma once

#include <Core/BaseSettings.h>
#include <Core/Defines.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MySQL/MySQLSettings.h>

namespace DB
{

class ASTStorage;

#define LIST_OF_CONNECTION_MYSQL_SETTINGS(M) \
    M(MySQLDataTypesSupport, mysql_datatypes_support_level, 0, "Which MySQL types should be converted to corresponding ClickHouse types (rather than being represented as String). Can be empty or any combination of 'decimal' or 'datetime64'. When empty MySQL's DECIMAL and DATETIME/TIMESTAMP with non-zero precision are seen as String on ClickHouse's side.", 0) \

/// Settings that should not change after the creation of a database.
#define APPLY_FOR_IMMUTABLE_CONNECTION_MYSQL_SETTINGS(M) \
    M(mysql_datatypes_support_level)

#define LIST_OF_MYSQL_DATABASE_SETTINGS(M) \
    LIST_OF_CONNECTION_MYSQL_SETTINGS(M) \
    LIST_OF_MYSQL_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(ConnectionMySQLSettingsTraits, LIST_OF_MYSQL_DATABASE_SETTINGS)


/** Settings for the MySQL database engine.
  * Could be loaded from a CREATE DATABASE query (SETTINGS clause) and Query settings.
  */
struct ConnectionMySQLSettings : public BaseSettings<ConnectionMySQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);

    void loadFromQueryContext(ContextPtr context);
};

}
