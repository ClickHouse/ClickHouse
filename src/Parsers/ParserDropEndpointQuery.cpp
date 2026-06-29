#include <Parsers/ASTDropEndpointQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropEndpointQuery.h>


namespace DB
{

bool ParserDropEndpointQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_endpoint(Keyword::ENDPOINT);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier name_p;

    if (!s_drop.ignore(pos, expected))
        return false;
    if (!s_endpoint.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr endpoint_ast;
    if (!name_p.parse(pos, endpoint_ast, expected))
        return false;

    auto query = make_intrusive<ASTDropEndpointQuery>();
    tryGetIdentifierNameInto(endpoint_ast, query->endpoint_name);
    query->if_exists = if_exists;
    node = query;
    return true;
}

}
