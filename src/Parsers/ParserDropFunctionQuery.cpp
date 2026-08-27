#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropFunctionQuery.h>

namespace DB
{

bool ParserDropFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_function(Keyword::FUNCTION);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier function_name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr function_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_function.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto drop_function_query = std::make_shared<ASTDropFunctionQuery>();
    drop_function_query->if_exists = if_exists;
    drop_function_query->cluster = std::move(cluster_str);

    node = drop_function_query;

    drop_function_query->function_name = function_name->as<ASTIdentifier &>().name();

    return true;
}

}
