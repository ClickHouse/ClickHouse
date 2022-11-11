#pragma once

#include <base/types.h>


namespace DB
{
class ASTFunction;

/// Finds arguments of a specified function which should not be displayed for most users for security reasons.
/// That involves passwords and secret keys.
/// The function returns a pair of numbers [first, last) specifying arguments which must be hidden.
/// If the function returns {-1, -1} that means no arguments must be hidden.
std::pair<size_t, size_t> findFunctionSecretArguments(const ASTFunction & function);

}
