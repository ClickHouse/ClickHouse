#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserCreateTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_as(Keyword::AS);
    auto s_input = ParserKeyword::createDeprecated("INPUT");
    auto s_output = ParserKeyword::createDeprecated("OUTPUT");
    ParserKeyword s_default(Keyword::DEFAULT);

    ParserIdentifier name_p;
    ParserDataType type_p;
    ParserIdentifier function_name_p;
    ParserExpression default_expression_p;
    ParserExpressionList params_p(false);

    ASTPtr name;
    ASTPtr base_type;
    ASTPtr params_ast;
    ASTPtr input_expression;
    ASTPtr output_expression;
    ASTPtr default_expression;
    bool if_not_exists = false;
    bool or_replace = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;
    else if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
    {
        if (!params_p.parse(pos, params_ast, expected))
            return false;
        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;
    }

    if (!s_as.ignore(pos, expected))
        return false;

    if (!type_p.parse(pos, base_type, expected))
        return false;

    if (s_input.ignore(pos, expected))
    {
        if (!function_name_p.parse(pos, input_expression, expected))
            return false;
    }

    if (s_output.ignore(pos, expected))
    {
        if (!function_name_p.parse(pos, output_expression, expected))
            return false;
    }

    if (s_default.ignore(pos, expected))
    {
        if (!default_expression_p.parse(pos, default_expression, expected))
            return false;
    }

    auto query = std::make_shared<ASTCreateTypeQuery>();
    query->name = typeid_cast<ASTIdentifier &>(*name).name();
    query->base_type = base_type;
    query->type_parameters = params_ast;
    query->input_expression = input_expression;
    query->output_expression = output_expression;
    query->default_expression = default_expression;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;

    node = query;
    return true;
}

}
