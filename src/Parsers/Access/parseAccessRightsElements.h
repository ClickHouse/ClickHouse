#pragma once

#include <Core/Types.h>
#include <Parsers/IParser.h>


namespace DB
{
class AccessFlags;
class AccessRightsElements;

/// Parses a list of privileges, for example "SELECT, INSERT".
bool parseAccessFlags(IParser::Pos & pos, Expected & expected, AccessFlags & access_flags);

/// Parses a list of privileges which can be written with lists of columns.
/// For example "SELECT(a), INSERT(b, c), DROP".
bool parseAccessFlagsWithColumns(IParser::Pos & pos, Expected & expected,
                                 std::vector<std::pair<AccessFlags, Strings>> & access_and_columns);

/// Parses a list of privileges with columns and tables or databases or wildcards,
/// For examples, "SELECT(a), INSERT(b,c) ON mydb.mytable, DROP ON mydb.*"
bool parseAccessRightsElementsWithoutOptions(IParser::Pos & pos, Expected & expected, AccessRightsElements & elements);

}
