#pragma once
#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db.]name
bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str);

/// After `db.table` has been parsed, consumes any further `.name` parts and folds them into
/// the table name: `db.ns1.ns2.table` -> table `ns1.ns2.table`.
/// Tables in DataLakeCatalog databases are named this way (namespace-qualified).
bool foldNamespacesIntoTableName(IParser::Pos & pos, Expected & expected, ASTPtr & table);

bool parseDatabaseAndTableAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & table);

/// Parses [db.]name or [db.]* or [*.]*
bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, String & table, bool & wildcard, bool & default_database);

bool parseDatabase(IParser::Pos & pos, Expected & expected, String & database_str);

bool parseDatabaseAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database);

}
