#pragma once
#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db.]name
bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str);

bool parseDatabaseAndTableAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & table);

/// Parses `[db.]table [FROM|IN db]` as `SHOW COLUMNS` and `SHOW INDEXES` spell their target: the table may be a
/// multipart path (folded as in `parseDatabaseAndTableAsAST`); the trailing clause is accepted only for a one-part name.
bool parseTableNameWithOptionalFromDatabase(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str);

/// Parses [db.]name or [db.]* or [*.]*
bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, String & table, bool & wildcard, bool & default_database);

bool parseDatabase(IParser::Pos & pos, Expected & expected, String & database_str);

bool parseDatabaseAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database);

}
