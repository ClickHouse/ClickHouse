#pragma once

#include <Core/Types.h>

#include <optional>


namespace DB
{

/// Returns the strings a regex like `a|b`, `(a|b)|(?:c)` or `api-(prod|stg)` matches if it consists only of
/// plain literals, alternations and groups. A group is expanded with the literals around it, so `api(-server|)`
/// gives `api-server` and `api`; an expansion of groups to more than 100 strings is not a list.
std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex);

}
