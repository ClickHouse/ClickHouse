#include <Parsers/ParserCreateFunctionQuery.h>

#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
//#include <base/logger_useful.h>


namespace DB
{

namespace {
    bool parseInterpFunctionArgs(IParser::Pos & pos, ASTPtr & args, Expected & expected){
        if (!ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
            return false;
        if (!ParserList{
                std::make_unique<ParserIdentifier>(),
                std::make_unique<ParserToken>(TokenType::Comma)}
                .parse(pos, args, expected))
            return false;
        if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
            return false;
        return true;
    }
}

bool ParserCreateFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    /** Two types of UDFs are supported:
     * 1) UDFs from lambdas
     * CREATE FUNCTION test AS x -> x || '1'
     * 2) UDFs using interpreted languages
     * CREATE FUNCTION name(args...) '
     *     body
     * ' USING interpreter
     */
    ParserKeyword s_create("CREATE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_function("FUNCTION");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserIdentifier function_name_p;
    ParserKeyword s_on("ON");
    ParserKeyword s_as("AS");

    ASTPtr function_name;

    String cluster_str;
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

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    std::shared_ptr<ASTCreateFunctionQuery> create_function_query;

    if (s_as.ignore(pos, expected))
    {
        ParserLambdaExpression lambda_p;
        ASTPtr function_core;
        if (!lambda_p.parse(pos, function_core, expected))
            return false;
        auto query = std::make_shared<ASTCreateLambdaFunctionQuery>();
        query->function_name = function_name;
        query->children.push_back(function_name);

        query->function_core = function_core;
        query->children.push_back(function_core);
        create_function_query = query;
    }
    else
    {
        ParserKeyword s_using("USING");
        ParserIdentifier interpreter_name_p;
        ParserStringLiteral function_body_p;
        ASTPtr function_args;
        ASTPtr function_body;
        ASTPtr interpreter_name;

        if (!parseInterpFunctionArgs(pos, function_args, expected))
            return false;
        if (!function_body_p.parse(pos, function_body, expected))
            return false;
        if (!s_using.ignore(pos, expected))
            return false;
        if (!interpreter_name_p.parse(pos, interpreter_name, expected))
            return false;
        auto query = std::make_shared<ASTCreateInterpFunctionQuery>();
        query->function_name = function_name;
        query->children.push_back(function_name);  // TODO remove duplicate code

        query->function_args = function_args;
        query->children.push_back(function_args);
        query->function_body = function_body;
        query->children.push_back(function_body);
        query->interpreter_name = interpreter_name;
        query->children.push_back(interpreter_name);

        create_function_query = query;
    }

    create_function_query->or_replace = or_replace;
    create_function_query->if_not_exists = if_not_exists;
    create_function_query->cluster = std::move(cluster_str);

    node = create_function_query;
    return true;
}

}
