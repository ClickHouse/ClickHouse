#include "KQLMathematicalFunctions.h"

#include <format>

namespace DB
{
bool IsNan::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos);
    out = std::format("if(toTypeName({0}) = 'Float64', isNaN({0}), throwIf(true, 'Expected argument of data type real'))", argument);

    return true;
}

bool Round::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "round");
}
}
