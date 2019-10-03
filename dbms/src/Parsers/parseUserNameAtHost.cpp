#include <Parsers/parseUserNameAtHost.h>


namespace DB
{
bool parseUserNameAtHost(Pos & pos, String & result, Expected & expected)
{
    return parseIdentifierOrStringLiteral(pos, expected, result);
}


bool parseRoleNameAtHost(Pos & pos, String & result, Expected & expected)
{
    return parseUserNameAtHost(pos, result, expected);
}
}
