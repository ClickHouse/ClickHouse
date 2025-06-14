#include <Parsers/Access/parseUserName.h>

#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

bool parseUserName(IParser::Pos & pos, Expected & expected, String & user_name, bool allow_query_parameter)
{
    ASTPtr ast;
    if (!ParserUserNameWithHost(allow_query_parameter).parse(pos, ast, expected))
        return false;
    user_name = ast->as<const ASTUserNameWithHost &>().toString();
    return true;
}


bool parseUserNames(IParser::Pos & pos, Expected & expected, Strings & user_names, bool allow_query_parameter)
{
    ASTPtr ast;
    if (!ParserUserNamesWithHost(allow_query_parameter).parse(pos, ast, expected))
        return false;
    user_names = ast->as<const ASTUserNamesWithHost &>().toStrings();
    return true;
}


bool parseCurrentUserTag(IParser::Pos & pos, Expected & expected)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        if (!ParserKeyword{Keyword::CURRENTUSER}.ignore(pos, expected) && !ParserKeyword{Keyword::CURRENT_USER}.ignore(pos, expected))
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
