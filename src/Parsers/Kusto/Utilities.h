#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParser.h>

namespace DB
{
String extractLiteralArgumentWithoutQuotes(const std::string & function_name, IParser::Pos & pos);
String extractTokenWithoutQuotes(IParser::Pos & pos);
void setSelectAll(ASTSelectQuery & select_query);
String wildcardToRegex(const String & wildcard);
ASTPtr wrapInSelectWithUnion(const ASTPtr & select_query);
bool isValidKQLPos(IParser::Pos & pos);
}
