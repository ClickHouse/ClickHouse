#include "Utilities.h"

#include "KustoFunctions/IParserKQLFunction.h"

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{
String extractLiteralArgumentWithoutQuotes(const std::string & function_name, IParser::Pos & pos)
{
    ++pos;
    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
    {
        auto result = extractTokenWithoutQuotes(pos);
        ++pos;
        return result;
    }

    --pos;
    return IParserKQLFunction::getArgument(function_name, pos, IParserKQLFunction::ArgumentState::Raw);
}

String extractTokenWithoutQuotes(IParser::Pos & pos)
{
    const auto offset = static_cast<int>(pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral);
    return {pos->begin + offset, pos->end - offset};
}

void setSelectAll(ASTSelectQuery & select_query)
{
    auto expression_list = std::make_shared<ASTExpressionList>();
    expression_list->children.push_back(std::make_shared<ASTAsterisk>());
    select_query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(expression_list));
}

String wildcardToRegex(const String & wildcard)
{
    String regex;
    for (char c : wildcard)
    {
        if (c == '*')
        {
            regex += ".*";
        }
        else if (c == '?')
        {
            regex += ".";
        }
        else if (c == '.' || c == '+' || c == '(' || c == ')' || c == '[' || c == ']' || c == '\\' || c == '^' || c == '$')
        {
            regex += "\\";
            regex += c;
        }
        else
        {
            regex += c;
        }
    }
    return regex;
}

ASTPtr wrapInSelectWithUnion(const ASTPtr & select_query)
{
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    auto & list_of_selects = select_with_union_query->list_of_selects;
    list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(select_query);
    select_with_union_query->children.push_back(list_of_selects);

    return select_with_union_query;
}

bool isValidKQLPos(IParser::Pos & pos)
{
    return (pos.isValid() ||
            pos->type == TokenType::ErrorSingleExclamationMark || // allow kql negative operators
            pos->type == TokenType::ErrorWrongNumber || // allow kql timespan data type with decimal like 2.6h
            std::string_view(pos->begin, pos->end) == "~");  // allow kql Case-Sensitive operators
}
}
