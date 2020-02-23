#include <Parsers/ParserGenericRoleSet.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTGenericRoleSet.h>
#include <Parsers/parseUserName.h>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
namespace
{
    bool parseBeforeExcept(IParserBase::Pos & pos, Expected & expected, bool * all, bool * current_user, Strings & names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            bool res_all = false;
            bool res_current_user = false;
            Strings res_names;
            while (true)
            {
                if (ParserKeyword{"NONE"}.ignore(pos, expected))
                {
                }
                else if (
                    current_user && (ParserKeyword{"CURRENT_USER"}.ignore(pos, expected) || ParserKeyword{"currentUser"}.ignore(pos, expected)))
                {
                    if (ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
                    {
                        if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                            return false;
                    }
                    res_current_user = true;
                }
                else if (all && ParserKeyword{"ALL"}.ignore(pos, expected))
                {
                    res_all = true;
                }
                else
                {
                    String name;
                    if (!parseUserName(pos, expected, name))
                        return false;
                    res_names.push_back(name);
                }

                if (!ParserToken{TokenType::Comma}.ignore(pos, expected))
                    break;
            }

            if (all)
                *all = res_all;
            if (current_user)
                *current_user = res_current_user;
            names = std::move(res_names);
            return true;
        });
    }

    bool parseExcept(IParserBase::Pos & pos, Expected & expected, bool * except_current_user, Strings & except_names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"EXCEPT"}.ignore(pos, expected))
                return false;

            return parseBeforeExcept(pos, expected, nullptr, except_current_user, except_names);
        });
    }
}


bool ParserGenericRoleSet::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Strings names;
    bool current_user = false;
    bool all = false;
    Strings except_names;
    bool except_current_user = false;

    if (!parseBeforeExcept(pos, expected, (allow_all ? &all : nullptr), (allow_current_user ? &current_user : nullptr), names))
        return false;

    parseExcept(pos, expected, (allow_current_user ? &except_current_user : nullptr), except_names);

    if (all)
        names.clear();

    auto result = std::make_shared<ASTGenericRoleSet>();
    result->names = std::move(names);
    result->current_user = current_user;
    result->all = all;
    result->except_names = std::move(except_names);
    result->except_current_user = except_current_user;
    node = result;
    return true;
}

}
