#include <Parsers/ParserCreateDriverFunctionQuery.h>

#include <Parsers/ASTCreateDriverFunctionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDataType.h>


namespace DB
{

bool ParserCreateDriverFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_function(Keyword::FUNCTION);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserIdentifier function_name_p;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserNameTypePairList function_params_p;
    ParserKeyword s_returns(Keyword::RETURNS);
    ParserDataType function_return_type_p;
    ParserKeyword s_engine(Keyword::ENGINE);
    ParserToken s_equals(TokenType::Equals);
    ParserIdentifier engine_name_p;
    ParserKeyword s_as(Keyword::AS);
    ParserToken s_dollar(TokenType::DollarSign);
    ParserStringLiteral function_body_p;

    ASTPtr function_name;
    ASTPtr function_params;
    ASTPtr function_return_type;
    ASTPtr engine_name;
    ASTPtr function_body;

    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_function.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    if (!s_lparen.ignore(pos, expected))
        return false;

    if (!function_params_p.parse(pos, function_params, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    if (!s_returns.ignore(pos, expected))
        return false;

    if (!function_return_type_p.parse(pos, function_return_type, expected))
        return false;

    if (!s_engine.ignore(pos, expected))
        return false;

    if (!s_equals.ignore(pos, expected))
        return false;

    if (!engine_name_p.parse(pos, engine_name, expected))
        return false;

    if (!s_as.ignore(pos, expected))
        return false;

    if (!function_body_p.parse(pos, function_body, expected))
        return false;

    auto create_function_query = std::make_shared<ASTCreateDriverFunctionQuery>();
    node = create_function_query;

    create_function_query->function_name = function_name;
    create_function_query->children.push_back(function_name);

    create_function_query->or_replace = or_replace;
    create_function_query->if_not_exists = if_not_exists;

    create_function_query->function_params = function_params;
    create_function_query->children.push_back(function_params);

    create_function_query->function_return_type = function_return_type;
    create_function_query->children.push_back(function_return_type);

    create_function_query->engine_name = engine_name;
    create_function_query->children.push_back(engine_name);

    create_function_query->function_body = function_body;
    create_function_query->children.push_back(function_body);

    return true;
}

}
