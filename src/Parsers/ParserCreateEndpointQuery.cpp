#include <Parsers/ASTCreateEndpointQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateEndpointQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>


namespace DB
{

bool ParserCreateEndpointQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_endpoint(Keyword::ENDPOINT);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserIdentifier name_p;

    if (!s_create.ignore(pos, expected))
        return false;
    if (!s_endpoint.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    ASTPtr endpoint_ast;
    if (!name_p.parse(pos, endpoint_ast, expected))
        return false;

    if (!s_properties.ignore(pos, expected))
        return false;

    SettingsChanges properties;
    if (!parseSQLClusterCatalogPropertiesAssignments(properties, pos, expected))
        return false;

    auto query = make_intrusive<ASTCreateEndpointQuery>();
    tryGetIdentifierNameInto(endpoint_ast, query->endpoint_name);
    query->properties = std::move(properties);
    query->if_not_exists = if_not_exists;
    node = query;
    return true;
}

}
