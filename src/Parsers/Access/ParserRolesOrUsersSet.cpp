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
}

bool ParserRolesOrUsersSet::parseBeforeExcept(
    IParserBase::Pos & pos,
    Expected & expected,
    bool & all,
    Strings & names,
    ASTRolesOrUsersSet::NameFilters & names_filters,
    bool & current_user) const
{
    bool res_all = false;
    Strings res_names;
    ASTRolesOrUsersSet::NameFilters res_names_filters;
    bool res_current_user = false;
    Strings res_with_roles_names;

    auto parse_element = [&]
    {
        if (ParserKeyword{"NONE"}.ignore(pos, expected))
            return true;

        if (allow_all && ParserKeyword{"ALL"}.ignore(pos, expected))
        {
            res_all = true;
            return true;
        }

        if (allow_any && ParserKeyword{"ANY"}.ignore(pos, expected))
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
            if (ParserKeyword{"AS USER"}.ignore(pos, expected))
            {
                if (allow_users)
                {
                    auto filter = res_names_filters.find(name);
                    if (filter == res_names_filters.end())
                    {
                        res_names_filters.emplace(name, ASTRolesOrUsersSet::NameFilter::USER);
                    }
                    else if (filter->second == ASTRolesOrUsersSet::NameFilter::ROLE)
                    {
                        filter->second = ASTRolesOrUsersSet::NameFilter::BOTH;
                    }
                }
                else
                {
                    return false;
                }
            }
            else if (ParserKeyword{"AS ROLE"}.ignore(pos, expected))
            {
                if (allow_roles)
                {
                    auto filter = res_names_filters.find(name);
                    if (filter == res_names_filters.end())
                    {
                        res_names_filters.emplace(name, ASTRolesOrUsersSet::NameFilter::ROLE);
                    }
                    else if (
                        filter->second == ASTRolesOrUsersSet::NameFilter::ANY || filter->second == ASTRolesOrUsersSet::NameFilter::USER)
                    {
                        filter->second = ASTRolesOrUsersSet::NameFilter::BOTH;
                    }
                }
                else
                {
                    return false;
                }
            }
            else if (ParserKeyword{"AS BOTH"}.ignore(pos, expected))
            {
                if (allow_roles && allow_users)
                {
                    auto filter = res_names_filters.find(name);
                    if (filter == res_names_filters.end())
                    {
                        res_names_filters.emplace(name, ASTRolesOrUsersSet::NameFilter::BOTH);
                    }
                    else
                    {
                        filter->second = ASTRolesOrUsersSet::NameFilter::BOTH;
                    }
                }
                else
                {
                    return false;
                }
            }
            res_names.emplace_back(std::move(name));
            return true;
        }
        return false;
    };

    if (!ParserList::parseUtil(pos, expected, parse_element, false))
        return false;

    names = std::move(res_names);
    names_filters = std::move(res_names_filters);
    current_user = res_current_user;
    all = res_all;
    return true;
}

bool ParserRolesOrUsersSet::parseExceptAndAfterExcept(
    IParserBase::Pos & pos,
    Expected & expected,
    Strings & except_names,
    ASTRolesOrUsersSet::NameFilters & except_names_filters,
    bool & except_current_user) const
{
    return IParserBase::wrapParseImpl(
        pos,
        [&]
        {
            if (!ParserKeyword{"EXCEPT"}.ignore(pos, expected))
                return false;

            bool unused;
            return parseBeforeExcept(pos, expected, unused, except_names, except_names_filters, except_current_user);
        });
}

bool ParserRolesOrUsersSet::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool all = false;
    Strings names;
    ASTRolesOrUsersSet::NameFilters names_filters;
    bool current_user = false;
    Strings except_names;
    ASTRolesOrUsersSet::NameFilters except_names_filters;
    bool except_current_user = false;

    if (!parseBeforeExcept(pos, expected, all, names, names_filters, current_user))
        return false;

    parseExceptAndAfterExcept(pos, expected, except_names, except_names_filters, except_current_user);

    if (all)
        names.clear();

    auto result = std::make_shared<ASTRolesOrUsersSet>();
    result->names = std::move(names);
    result->names_filters = std::move(names_filters);
    result->current_user = current_user;
    result->all = all;
    result->except_names = std::move(except_names);
    result->except_names_filters = std::move(except_names_filters);
    result->except_current_user = except_current_user;
    result->enable_extended_subject_syntax = true;
    result->allow_users = allow_users;
    result->allow_roles = allow_roles;
    result->id_mode = id_mode;
    result->use_keyword_any = all && allow_any && !allow_all;
    node = result;
    return true;
}

}
