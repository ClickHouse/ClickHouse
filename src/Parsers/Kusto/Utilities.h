#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParser.h>
#include <unordered_map>

namespace DB
{
String extractLiteralArgumentWithoutQuotes(const std::string & function_name, IParser::Pos & pos);
String extractTokenWithoutQuotes(IParser::Pos & pos);
void setSelectAll(ASTSelectQuery & select_query);
String wildcardToRegex(const String & wildcard);
ASTPtr wrapInSelectWithUnion(const ASTPtr & select_query);
bool isValidKQLPos(IParser::Pos & pos);

/// KQL let statement bindings (thread-local storage for variable substitution)
std::unordered_map<String, String> & kqlLetBindings();
void kqlLetBindingsClear();
}
