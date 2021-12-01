#include <Parsers/parseUserName.h>
#include <Parsers/ParserUserNameWithHost.h>
#include <Parsers/ASTUserNameWithHost.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool parseUserName(IParser::Pos & pos, Expected & expected, String & user_name)
{
    ASTPtr ast;
    if (!ParserUserNameWithHost{}.parse(pos, ast, expected))
        return false;
    user_name = ast->as<const ASTUserNameWithHost &>().toString();
    return true;
}


bool parseUserNames(IParser::Pos & pos, Expected & expected, Strings & user_names)
{
    ASTPtr ast;
    if (!ParserUserNamesWithHost{}.parse(pos, ast, expected))
        return false;
    user_names = ast->as<const ASTUserNamesWithHost &>().toStrings();
    return true;
}


bool parseCurrentUserTag(IParser::Pos & pos, Expected & expected)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (!ParserKeyword{"CURRENT_USER"}.ignore(pos, expected) && !ParserKeyword{"currentUser"}.ignore(pos, expected))
            return false;

        if (ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
        {
            if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                return false;
        }
        return true;
    });
}

}
