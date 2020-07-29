#include <Parsers/ParserSetRoleQuery.h>
#include <Parsers/ASTSetRoleQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTGenericRoleSet.h>
#include <Parsers/ParserGenericRoleSet.h>


namespace DB
{
namespace
{
    bool parseRoles(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTGenericRoleSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!ParserGenericRoleSet{}.enableCurrentUserKeyword(false).parse(pos, ast, expected))
                return false;

            roles = typeid_cast<std::shared_ptr<ASTGenericRoleSet>>(ast);
            return true;
        });
    }

    bool parseToUsers(IParserBase::Pos & pos, Expected & expected, std::shared_ptr<ASTGenericRoleSet> & to_users)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"TO"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            if (!ParserGenericRoleSet{}.enableAllKeyword(false).parse(pos, ast, expected))
                return false;

            to_users = typeid_cast<std::shared_ptr<ASTGenericRoleSet>>(ast);
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

    std::shared_ptr<ASTGenericRoleSet> roles;
    std::shared_ptr<ASTGenericRoleSet> to_users;

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
