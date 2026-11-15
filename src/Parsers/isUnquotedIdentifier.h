#pragma once

#include <base/types.h>

namespace DB
{

/// Checks if the input string @name is a valid unquoted identifier.
///
/// Example Usage:
///   abc     -> true   (valid unquoted identifier)
///   123     -> false  (identifiers cannot start with digits)
///   `123`   -> false  (quoted identifiers are not considered)
///   `abc`   -> false  (quoted identifiers are not considered)
///   null    -> false  (reserved literal keyword)
bool isUnquotedIdentifier(const String & name);

}
