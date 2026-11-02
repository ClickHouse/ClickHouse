#pragma once
#include <Core/IdentifierName.h>
#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db.]name
bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str);

/// Parses [db.]name, also reporting how each part was quoted.
bool parseDatabaseAndTableName(
    IParser::Pos & pos, Expected & expected, String & database_str, String & table_str,
    IdentifierPartQuote & database_quote, IdentifierPartQuote & table_quote);

bool parseDatabaseAndTableAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & table);

/// Parses [db.]name or [db.]* or [*.]*
bool parseDatabaseAndTableNameOrAsterisks(IParser::Pos & pos, Expected & expected, String & database, String & table, bool & wildcard, bool & default_database);

bool parseDatabase(IParser::Pos & pos, Expected & expected, String & database_str);

bool parseDatabaseAsAST(IParser::Pos & pos, Expected & expected, ASTPtr & database);

}
