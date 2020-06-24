#pragma once

#include <Parsers/IParser.h>


namespace DB
{
/// Parses a user name. It can be a simple string or identifier or something like `name@host`.
/// In the last case `host` specifies the hosts user is allowed to connect from.
/// The `host` can be an ip address, ip subnet, or a host name.
/// The % and _ wildcard characters are permitted in `host`.
/// These have the same meaning as for pattern-matching operations performed with the LIKE operator.
bool parseUserName(IParser::Pos & pos, Expected & expected, String & user_name, std::optional<String> & host_like_pattern);
bool parseUserName(IParser::Pos & pos, Expected & expected, String & user_name);

/// Parses either a user name or the 'CURRENT_USER' keyword (or some of the aliases).
bool parseUserNameOrCurrentUserTag(IParser::Pos & pos, Expected & expected, String & user_name, bool & current_user);

/// Parses a role name. It follows the same rules as a user name, but allowed hosts are never checked
/// (because roles are not used to connect to server).
inline bool parseRoleName(IParser::Pos & pos, Expected & expected, String & role_name)
{
    return parseUserName(pos, expected, role_name);
}
}
