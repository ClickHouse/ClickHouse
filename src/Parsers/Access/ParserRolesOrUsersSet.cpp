#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
namespace
{
    bool parseNameOrID(IParserBase::Pos & pos, Expected & expected, bool id_mode, bool allow_query_parameter, ASTPtr & res)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!id_mode)
                return ParserUserNameWithHost(allow_query_parameter, /*parse_host_pattern=*/ false).parse(pos, res, expected);

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

            res = make_intrusive<ASTUserNameWithHost>(id);
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
        ASTs & names,
        bool & current_user,
        bool allow_query_parameter)
    {
        bool res_all = false;
        ASTs res_names;
        bool res_current_user = false;

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

            ASTPtr name;
            if (parseNameOrID(pos, expected, id_mode, allow_query_parameter, name))
            {
                res_names.push_back(std::move(name));
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
        ASTs & except_names,
        bool & except_current_user,
        bool allow_query_parameter)
    {
        return IParserBase::wrapParseImpl(pos, [&] {
            if (!ParserKeyword{Keyword::EXCEPT}.ignore(pos, expected))
                return false;

            bool unused = false;
            return parseBeforeExcept(pos, expected, id_mode, false, false, allow_current_user, unused, except_names, except_current_user, allow_query_parameter);
        });
    }
}


bool ParserRolesOrUsersSet::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool all = false;
    ASTs names;
    bool current_user = false;
    ASTs except_names;
    bool except_current_user = false;

    if (!parseBeforeExcept(pos, expected, id_mode, allow_all, allow_any, allow_current_user, all, names, current_user, allow_query_parameter))
        return false;

    parseExceptAndAfterExcept(pos, expected, id_mode, allow_current_user, except_names, except_current_user, allow_query_parameter);

    if (all)
    {
        for (const auto & name : names)
            if (name->as<const ASTUserNameWithHost &>().usernameWasQueryParameter())
                return false;

        names.clear();
    }

    auto result = make_intrusive<ASTRolesOrUsersSet>();
    result->current_user = current_user;
    result->all = all;
    result->except_current_user = except_current_user;
    result->allow_users = allow_users;
    result->allow_roles = allow_roles;
    result->id_mode = id_mode;
    result->use_keyword_any = all && allow_any && !allow_all;

    if (!names.empty())
    {
        result->names = make_intrusive<ASTUserNamesWithHost>();
        result->names->children.swap(names);
        if (result->names->hasQueryParameters())
            result->children.push_back(result->names);
    }

    if (!except_names.empty())
    {
        result->except_names = make_intrusive<ASTUserNamesWithHost>();
        result->except_names->children.swap(except_names);
        if (result->except_names->hasQueryParameters())
            result->children.push_back(result->except_names);
    }

    node = result;
    return true;
}

}
