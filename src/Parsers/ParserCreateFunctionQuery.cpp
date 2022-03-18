#include <Parsers/ParserCreateFunctionQuery.h>

#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>


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

    bool parseInterpFunctionBody(IParser::Pos & pos, [[maybe_unused]] ASTPtr & body, Expected & expected){
        if (!ParserToken{TokenType::OpeningCurlyBrace}.ignore(pos, expected))
            return false;
        // !! body
        if (!ParserToken{TokenType::ClosingCurlyBrace}.ignore(pos, expected))
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
     * CREATE [VOLATILE] FUNCTION name(args...) {
     *     body
     * } USING interpreter
     */
    ParserKeyword s_create("CREATE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_volatile("VOLATILE");  /// Has no effect on type-1 UDFs
    ParserKeyword s_function("FUNCTION");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserIdentifier function_name_p;
    ParserKeyword s_on("ON");
    /// type-1
    ParserKeyword s_as("AS");
    ParserLambdaExpression lambda_p;
    /// type-2
    ParserKeyword s_using("USING");
    ParserIdentifier interpreter_name_p;

    ASTPtr function_name;
    ASTPtr function_args;  /// only for type-2 UDFs. !! optimize?
    ASTPtr function_core;
    ASTPtr interpreter_name;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;
    // bool is_volatile = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

     if (s_volatile.ignore(pos, expected)) {
     //    is_volatile = true;
     }

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

    if (s_as.ignore(pos, expected))
    {
        if (!lambda_p.parse(pos, function_core, expected))
            return false;
    }
    else
    {
        if (!parseInterpFunctionArgs(pos, function_args, expected))
            return false;
        if (!parseInterpFunctionBody(pos, function_core, expected))
            return false;
        if (!s_using.ignore(pos, expected))
            return false;
        if (!interpreter_name_p.parse(pos, interpreter_name, expected))
            return false;
    }

    auto create_function_query = std::make_shared<ASTCreateFunctionQuery>();  // !! subclass?
    node = create_function_query;

    create_function_query->function_name = function_name;
    create_function_query->children.push_back(function_name);

    create_function_query->function_core = function_core;
    create_function_query->children.push_back(function_core);

    // !! args, interpreter

    create_function_query->or_replace = or_replace;
    create_function_query->if_not_exists = if_not_exists;
    create_function_query->cluster = std::move(cluster_str);

    return true;
}

}
