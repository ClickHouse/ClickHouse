#include <Parsers/ASTAlterEndpointQuery.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserAlterEndpointQuery.h>
#include <Parsers/ParserSQLClusterCatalogProperties.h>


namespace DB
{

bool ParserAlterEndpointQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_endpoint(Keyword::ENDPOINT);
    ParserKeyword s_modify(Keyword::MODIFY);
    ParserKeyword s_properties(Keyword::PROPERTIES);
    ParserIdentifier name_p;

    if (!s_alter.ignore(pos, expected))
        return false;
    if (!s_endpoint.ignore(pos, expected))
        return false;

    ASTPtr endpoint_ast;
    if (!name_p.parse(pos, endpoint_ast, expected))
        return false;
    if (!s_modify.ignore(pos, expected))
        return false;
    if (!s_properties.ignore(pos, expected))
        return false;

    SettingsChanges properties;
    if (!parseSQLClusterCatalogPropertiesAssignments(properties, pos, expected))
        return false;

    auto query = make_intrusive<ASTAlterEndpointQuery>();
    tryGetIdentifierNameInto(endpoint_ast, query->endpoint_name);
    query->properties = std::move(properties);
    node = query;
    return true;
}

}
