#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ParserRolesOrUsersSet.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>


namespace DB
{
bool ParserShowGrantsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW GRANTS"}.ignore(pos, expected))
        return false;

    std::shared_ptr<ASTRolesOrUsersSet> for_roles;

    if (ParserKeyword{"FOR"}.ignore(pos, expected))
    {
        ASTPtr for_roles_ast;
        ParserRolesOrUsersSet for_roles_p;
        for_roles_p.allowUserNames().allowRoleNames().allowAll().allowCurrentUser();
        if (!for_roles_p.parse(pos, for_roles_ast, expected))
            return false;

        for_roles = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(for_roles_ast);
    }
    else
    {
        for_roles = std::make_shared<ASTRolesOrUsersSet>();
        for_roles->current_user = true;
    }

    auto query = std::make_shared<ASTShowGrantsQuery>();
    query->for_roles = std::move(for_roles);
    node = query;

    return true;
}
}
