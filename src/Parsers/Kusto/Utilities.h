#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParser.h>
#include <Parsers/Kusto/IKQLParser.h>
namespace DB
{
void setSelectAll(ASTSelectQuery & select_query);
String wildcardToRegex(const String & wildcard);
ASTPtr wrapInSelectWithUnion(const ASTPtr & select_query);
bool isValidKQLPos(IKQLParser::KQLPos & pos);
}
