#include "ASTCreateDataTypeQuery.h"
#include "CommonParsers.h"
#include "ExpressionElementParsers.h"
#include "ParserCreateDataTypeQuery.h"
#include "ParserDataType.h"
#include "ASTIdentifier.h"

namespace DB
{
bool ParserCreateDataTypeQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_type("TYPE");
    ParserIdentifier type_name_p;
    ParserKeyword s_as("AS");
    ParserKeyword s_with("WITH");
    ParserDataType nested_p;
    ParserToken s_open(TokenType::OpeningRoundBracket);
    ParserToken s_close(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);
    ParserFunctionDefinitionQuery input_function_p("INPUT");
    ParserFunctionDefinitionQuery output_function_p("OUTPUT");

    ASTPtr type_name;
    ASTPtr nested;
    ASTPtr input_function;
    ASTPtr output_function;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (!type_name_p.parse(pos, type_name, expected))
        return false;

    if (!s_as.ignore(pos, expected))
        return false;

    if (!nested_p.parse(pos, nested, expected))
        return false;

    if (!s_with.ignore(pos, expected))
        return false;
    if (!s_open.ignore(pos, expected))
        return false;

    if (!input_function_p.parse(pos, input_function, expected))
        return false;

    if (!s_comma.ignore(pos, expected))
        return false;

    if (!output_function_p.parse(pos, output_function, expected))
        return false;

    if (!s_close.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTCreateDataTypeQuery>();
    node = query;
    query->type_name = type_name->as<ASTIdentifier &>().name();
    query->nested = nested;
    query->input_function = input_function->as<ASTIdentifier &>().name();
    query->output_function = output_function->as<ASTIdentifier &>().name();

    return true;
}

bool ParserFunctionDefinitionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_function_type(function_type_name.c_str());
    ParserToken s_equals(TokenType::Equals);
    ParserIdentifier function_name_p;

    ASTPtr function_name;

    if (!s_function_type.ignore(pos, expected))
        return false;

    if (!s_equals.ignore(pos, expected))
        return false;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    node = function_name;
    return true;
}

}
