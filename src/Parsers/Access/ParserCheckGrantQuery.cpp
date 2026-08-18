#include <Parsers/Access/ParserCheckGrantQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/parseAccessRightsElements.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


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

    auto query = make_intrusive<ASTCheckGrantQuery>();
    node = query;

    query->access_rights_elements = std::move(elements);

    return true;
}
}

namespace DB
{

void registerStatementCheckGrant(StatementFactory & factory)
{
    factory.registerStatement("CHECK GRANT",
    {
        .description = R"(
Checks whether the current user or role has been granted a specific privilege. Returns `1` if the privilege is granted
and `0` otherwise. A privilege on a table or a column which does not exist raises an exception.
)",
        .syntax = R"(
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
)",
        .examples = {{"Check a privilege on a column", "CHECK GRANT SELECT(col1) ON table_1;", "1"}},
        .related = {"GRANT", "REVOKE", "SHOW"},
    });
}

}
