#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
namespace
{
    bool parseNameOrID(IParserBase::Pos & pos, Expected & expected, bool id_mode, String & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!id_mode)
                return parseRoleName(pos, expected, res);

            if (!ParserKeyword{Keyword::ID}.ignore(pos, expected))
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
        bool allow_any,
        bool allow_current_user,
        bool & all,
        Strings & names,
        bool & current_user)
    {
        bool res_all = false;
        Strings res_names;
        bool res_current_user = false;
        Strings res_with_roles_names;

        auto parse_element = [&]
        {
            if (ParserKeyword{Keyword::NONE}.ignore(pos, expected))
                return true;

            if (allow_all && ParserKeyword{Keyword::ALL}.ignore(pos, expected))
            {
                res_all = true;
                return true;
            }

            if (allow_any && ParserKeyword{Keyword::ANY}.ignore(pos, expected))
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
            if (parseNameOrID(pos, expected, id_mode, name))
            {
                res_names.emplace_back(std::move(name));
                return true;
            }

            return false;
        };

        if (!ParserList::parseUtil(pos, expected, parse_element, false))
            return false;

        names = std::move(res_names);
        current_user = res_current_user;
        all = res_all;
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
        return IParserBase::wrapParseImpl(pos, [&] {
            if (!ParserKeyword{Keyword::EXCEPT}.ignore(pos, expected))
                return false;

            bool unused;
            return parseBeforeExcept(pos, expected, id_mode, false, false, allow_current_user, unused, except_names, except_current_user);
        });
    }
}


bool ParserRolesOrUsersSet::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool all = false;
    Strings names;
    bool current_user = false;
    Strings except_names;
    bool except_current_user = false;

    if (!parseBeforeExcept(pos, expected, id_mode, allow_all, allow_any, allow_current_user, all, names, current_user))
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
    result->allow_users = allow_users;
    result->allow_roles = allow_roles;
    result->id_mode = id_mode;
    result->use_keyword_any = all && allow_any && !allow_all;
    node = result;
    return true;
}

}
