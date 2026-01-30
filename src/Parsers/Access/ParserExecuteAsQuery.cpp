#include <Parsers/Access/ParserExecuteAsQuery.h>

#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

ParserExecuteAsQuery::ParserExecuteAsQuery(IParser & subquery_parser_)
    : subquery_parser(subquery_parser_)
{
}

bool ParserExecuteAsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::EXECUTE_AS}.ignore(pos, expected))
        return false;

    ASTPtr target_user;
    if (!ParserUserNameWithHost(/*allow_query_parameter=*/ false).parse(pos, target_user, expected))
        return false;

    auto query = std::make_shared<ASTExecuteAsQuery>();
    node = query;

    query->set(query->target_user, target_user);

    /// support 1) EXECUTE AS <user1>  2) EXECUTE AS <user1> SELECT ...

    ASTPtr subquery;
    if (subquery_parser.parse(pos, subquery, expected))
        query->set(query->subquery, subquery);

    return true;
}

}
