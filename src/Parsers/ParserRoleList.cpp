#include <Parsers/ParserRoleList.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <boost/range/algorithm/find.hpp>


namespace DB
{

bool ParserRoleList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Strings roles;
    bool current_user = false;
    bool all_roles = false;
    Strings except_roles;
    bool except_current_user = false;

    bool except_mode = false;
    while (true)
    {
        if (ParserKeyword{"NONE"}.ignore(pos, expected))
        {
        }
        else if (ParserKeyword{"CURRENT_USER"}.ignore(pos, expected) ||
                 ParserKeyword{"currentUser"}.ignore(pos, expected))
        {
            if (ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
            {
                if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                    return false;
            }
            if (except_mode && !current_user)
                except_current_user = true;
            else
                current_user = true;
        }
        else if (ParserKeyword{"ALL"}.ignore(pos, expected))
        {
            all_roles = true;
            if (ParserKeyword{"EXCEPT"}.ignore(pos, expected))
            {
                except_mode = true;
                continue;
            }
        }
        else
        {
            String name;
            if (!parseIdentifierOrStringLiteral(pos, expected, name))
                return false;
            if (except_mode && (boost::range::find(roles, name) == roles.end()))
                except_roles.push_back(name);
            else
                roles.push_back(name);
        }

        if (!ParserToken{TokenType::Comma}.ignore(pos, expected))
            break;
    }

    if (all_roles)
    {
        current_user = false;
        roles.clear();
    }

    auto result = std::make_shared<ASTRoleList>();
    result->roles = std::move(roles);
    result->current_user = current_user;
    result->all_roles = all_roles;
    result->except_roles = std::move(except_roles);
    result->except_current_user = except_current_user;
    node = result;
    return true;
}

}
