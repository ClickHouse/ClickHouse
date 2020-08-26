#include <Parsers/parseUserName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/CommonParsers.h>
#include <boost/algorithm/string.hpp>


namespace DB
{
bool parseUserName(IParser::Pos & pos, Expected & expected, String & user_name, std::optional<String> & host_like_pattern)
{
    String name;
    if (!parseIdentifierOrStringLiteral(pos, expected, name))
        return false;

    boost::algorithm::trim(name);

    std::optional<String> pattern;
    if (ParserToken{TokenType::At}.ignore(pos, expected))
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, pattern.emplace()))
            return false;

        boost::algorithm::trim(*pattern);
    }

    if (pattern && (pattern != "%"))
        name += '@' + *pattern;

    user_name = std::move(name);
    host_like_pattern = std::move(pattern);
    return true;
}


bool parseUserName(IParser::Pos & pos, Expected & expected, String & user_name)
{
    std::optional<String> unused_pattern;
    return parseUserName(pos, expected, user_name, unused_pattern);
}


bool parseUserNameOrCurrentUserTag(IParser::Pos & pos, Expected & expected, String & user_name, bool & current_user)
{
    if (ParserKeyword{"CURRENT_USER"}.ignore(pos, expected) || ParserKeyword{"currentUser"}.ignore(pos, expected))
    {
        if (ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
        {
            if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                return false;
        }
        current_user = true;
        return true;
    }

    if (parseUserName(pos, expected, user_name))
    {
        current_user = false;
        return true;
    }

    return false;
}

}
