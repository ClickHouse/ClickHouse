#include <Parsers/Access/ParserShowGrantsQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/CommonParsers.h>


namespace DB
{
bool ParserShowGrantsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::SHOW_GRANTS}.ignore(pos, expected))
        return false;

    std::shared_ptr<ASTRolesOrUsersSet> for_roles;

    if (ParserKeyword{Keyword::FOR}.ignore(pos, expected))
    {
        ASTPtr for_roles_ast;
        ParserRolesOrUsersSet for_roles_p;
        for_roles_p.allowUsers().allowRoles().allowAll().allowCurrentUser();
        if (!for_roles_p.parse(pos, for_roles_ast, expected))
            return false;

        for_roles = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(for_roles_ast);
    }
    else
    {
        for_roles = std::make_shared<ASTRolesOrUsersSet>();
        for_roles->current_user = true;
    }

    bool with_implicit = false;
    if (ParserKeyword{Keyword::WITH_IMPLICIT}.ignore(pos, expected))
        with_implicit = true;

    auto query = std::make_shared<ASTShowGrantsQuery>();
    query->for_roles = std::move(for_roles);
    query->with_implicit = with_implicit;
    node = query;

    return true;
}
}
