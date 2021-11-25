#include <Parsers/ParserRolesOrUsersSet.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/parseUserName.h>
#include <Parsers/ExpressionListParsers.h>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
namespace
{
    bool parseRoleNameOrID(
        IParserBase::Pos & pos,
        Expected & expected,
        bool id_mode,
        String & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!id_mode)
                return parseRoleName(pos, expected, res);

            if (!ParserKeyword{"ID"}.ignore(pos, expected))
                return false;
            if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
                return false;
            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            String id = ast->as<ASTLiteral &>().value.safeGet<String>();
            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;

            res = std::move(id);
            return true;
        });
    }


    bool parseBeforeExcept(
        IParserBase::Pos & pos,
        Expected & expected,
        bool id_mode,
        bool allow_all,
        bool allow_current_user,
        Strings & names,
        bool & all,
        bool & current_user)
    {
        bool res_all = false;
        bool res_current_user = false;
        Strings res_names;

        auto parse_element = [&]
        {
            if (ParserKeyword{"NONE"}.ignore(pos, expected))
                return true;

            if (allow_all && ParserKeyword{"ALL"}.ignore(pos, expected))
            {
                res_all = true;
                return true;
            }

            if (allow_current_user && parseCurrentUserTag(pos, expected))
            {
                res_current_user = true;
                return true;
            }

            String name;
            if (parseRoleNameOrID(pos, expected, id_mode, name))
            {
                res_names.emplace_back(std::move(name));
                return true;
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_element, false))
            return false;

        names = std::move(res_names);
        all = res_all;
        current_user = res_current_user;
        return true;
    }

    bool parseExceptAndAfterExcept(
        IParserBase::Pos & pos,
        Expected & expected,
        bool id_mode,
        bool allow_current_user,
        Strings & except_names,
        bool & except_current_user)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"EXCEPT"}.ignore(pos, expected))
                return false;

            bool unused;
            return parseBeforeExcept(pos, expected, id_mode, false, allow_current_user, except_names, unused, except_current_user);
        });
    }
}


bool ParserRolesOrUsersSet::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Strings names;
    bool current_user = false;
    bool all = false;
    Strings except_names;
    bool except_current_user = false;

    if (!parseBeforeExcept(pos, expected, id_mode, allow_all, allow_current_user, names, all, current_user))
        return false;

    parseExceptAndAfterExcept(pos, expected, id_mode, allow_current_user, except_names, except_current_user);

    if (all)
        names.clear();

    auto result = std::make_shared<ASTRolesOrUsersSet>();
    result->names = std::move(names);
    result->current_user = current_user;
    result->all = all;
    result->except_names = std::move(except_names);
    result->except_current_user = except_current_user;
    result->id_mode = id_mode;
    result->allow_user_names = allow_user_names;
    result->allow_role_names = allow_role_names;
    node = result;
    return true;
}

}
