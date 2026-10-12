#pragma once
#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db.]name, where the name may be hierarchical: `a.b.c` is the table `c` of the database `a.b`, and the
/// interpreter resolves the other splits against the catalog (see `DatabaseCatalog::resolveHierarchicalName`).
bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str);

/// Parses [db.]name with at most two parts, for the statements that do not resolve hierarchical names
/// (`BACKUP`, `RESTORE`, `SNAPSHOT`): `a.b.c` is not accepted unquoted there.
bool parseSimpleDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str);

bool parseDatabaseAndTableAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & table);

/// Parses [db.]name or [db.]* or [*.]*
bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, String & table, bool & wildcard, bool & default_database);

bool parseDatabase(IParser::Pos & pos, Expected & expected, String & database_str);

bool parseDatabaseAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database);

}
