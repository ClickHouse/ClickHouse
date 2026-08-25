#include <Parsers/Access/ParserCheckGrantQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/parseAccessRightsElements.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool ParserCheckGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::CHECK_GRANT}.ignore(pos, expected))
        return false;

    AccessRightsElements elements;
    if (!parseAccessRightsElementsWithoutOptions(pos, expected, elements))
        return false;

    elements.throwIfNotGrantable();

    auto query = std::make_shared<ASTCheckGrantQuery>();
    node = query;

    query->access_rights_elements = std::move(elements);

    return true;
}
}
