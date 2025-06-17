#include "KQLMathematicalFunctions.h"

#include <fmt/format.h>

namespace DB
{
bool IsNan::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos);
    out = fmt::format("if(toTypeName({0}) = 'Float64', isNaN({0}), throwIf(true, 'Expected argument of data type real'))", argument);

    return true;
}

bool Round::convertImpl(String & out, IKQLParser::KQLPos & pos)
{
    return directMapping(out, pos, "round");
}
}
