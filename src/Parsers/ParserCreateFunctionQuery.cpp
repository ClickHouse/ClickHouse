#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateFunctionQuery.h>

namespace DB
{

bool ParserCreateFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_function("FUNCTION");
    ParserIdentifier function_name_p;
    ParserKeyword s_as("AS");
    ParserLambdaExpression lambda_p;

    ASTPtr function_name;
    ASTPtr function_core;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_function.ignore(pos, expected))
        return false;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    if (!s_as.ignore(pos, expected))
        return false;

    if (!lambda_p.parse(pos, function_core, expected))
        return false;

    auto create_function_query = std::make_shared<ASTCreateFunctionQuery>();
    node = create_function_query;

    create_function_query->function_name = function_name->as<ASTIdentifier &>().name();
    create_function_query->function_core = function_core;

    return true;
}

}
