#pragma once

#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/SettingsCommon.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

class Context;
class ASTStorage;
class ASTSetQuery;
using ASTSetQueryPtr = std::shared_ptr<ASTSetQuery>;

/** Settings for the MySQL engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  * Or Cloud be loaded from config.xml (mysql -> mysql_database_name -> mysql_database_name_table_name)
  */
struct MySQLSettings
{
public:
    MySQLSettings(const ASTStorage & storage_def, const Context & context, const Poco::Util::AbstractConfiguration & config,
                  const String & default_database, const String & default_table);

    ASTSetQueryPtr set_variables_query = std::make_shared<ASTSetQuery>();

#define APPLY_FOR_MySQL_SETTINGS(M) \
    M(SettingString, remote_address, "", "Address of the MySQL server.") \
    M(SettingString, remote_database, "", "Database name on the MySQL server.") \
    M(SettingString, remote_table_name, "", "A Table name on the MySQL server.") \
    M(SettingString, user, "", "The MySQL User") \
    M(SettingString, password, "", "User password") \
    M(SettingBool, replace_query, 0, "Flag that sets query substitution INSERT INTO to REPLACE INTO. If replace_query=1, the query is replaced") \
    M(SettingString, on_duplicate_clause, "", "Adds the ON DUPLICATE KEY on_duplicate_clause expression to the INSERT query") \

#define DECLARE(TYPE, NAME, DEFAULT, DESCRIPTION) \
    TYPE NAME {DEFAULT};

    APPLY_FOR_MySQL_SETTINGS(DECLARE)

#undef DECLARE

    String genSessionVariablesQuery();

private:
    void evaluateSetQuery(const ASTSetQuery * set_query);

    void evaluateEngineArguments(const ASTs & arguments, const Context & context);

    ASTPtr convertConfigToSetQuery(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const;

    void fillDefaultDatabaseAndTableName(const String & default_database, const String & default_table_name);

    template<typename SettingType>
    void evaluateEngineArguments(const String & name, SettingType & setting, const ASTPtr & argument, const Context & context);
};

}