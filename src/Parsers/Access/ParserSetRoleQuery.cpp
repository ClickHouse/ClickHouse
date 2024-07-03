#include <Parsers/Access/ParserSetRoleQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/CommonParsers.h>


namespace DB
{
namespace
{
    bool parseRoles(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserRolesOrUsersSet roles_p;
            roles_p.allowRoles().allowAll();
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
            roles->allow_users = false;
            return true;
        });
    }

    bool parseToUsers(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTRolesOrUsersSet> & to_users)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"TO"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserRolesOrUsersSet users_p;
            users_p.allowUsers().allowCurrentUser();
            if (!users_p.parse(pos, ast, expected))
                return false;

            to_users = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
            to_users->allow_roles = false;
            return true;
        });
    }
}


bool ParserSetRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    using Kind = ASTSetRoleQuery::Kind;
    Kind kind;
    if (ParserKeyword{"SET ROLE DEFAULT"}.ignore(pos, expected))
        kind = Kind::SET_ROLE_DEFAULT;
    else if (ParserKeyword{"SET ROLE"}.ignore(pos, expected))
        kind = Kind::SET_ROLE;
    else if (ParserKeyword{"SET DEFAULT ROLE"}.ignore(pos, expected))
        kind = Kind::SET_DEFAULT_ROLE;
    else
        return false;

    std::shared_ptr<ASTRolesOrUsersSet> roles;
    std::shared_ptr<ASTRolesOrUsersSet> to_users;

    if ((kind == Kind::SET_ROLE) || (kind == Kind::SET_DEFAULT_ROLE))
    {
        if (!parseRoles(pos, expected, roles))
            return false;

        if (kind == Kind::SET_DEFAULT_ROLE)
        {
            if (!parseToUsers(pos, expected, to_users))
                return false;
        }
    }

    auto query = std::make_shared<ASTSetRoleQuery>();
    node = query;

    query->kind = kind;
    query->roles = std::move(roles);
    query->to_users = std::move(to_users);

    return true;
}
}
