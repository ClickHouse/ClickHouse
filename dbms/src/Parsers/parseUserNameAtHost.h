#pragma once


namespace DB
{
/// Parses a string like
/// name[@host]
bool parseUserNameAtHost(Pos & pos, String & result, Expected & expected);
bool parseRoleNameAtHost(Pos & pos, String & result, Expected & expected);
}
