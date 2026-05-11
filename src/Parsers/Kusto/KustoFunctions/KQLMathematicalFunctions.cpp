#include <Parsers/Kusto/KustoFunctions/KQLMathematicalFunctions.h>

#include <fmt/format.h>

namespace DB
{
bool IsNan::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto argument = getArgument(function_name, pos);
    out = fmt::format("toBool(if(toTypeName({0}) = 'Float64', isNaN({0}), throwIf(true, 'Expected argument of data type real')))", argument);

    return true;
}

bool Round::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "round");
}

bool Abs::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "abs");
}

bool Sqrt::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sqrt");
}

bool Pow::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "pow");
}

bool Log::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "log");
}

bool Log10::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "log10");
}

bool Log2::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "log2");
}

bool Sign::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "sign");
}

bool IsFinite::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos);
    out = fmt::format("toBool(isFinite({}))", arg);
    return true;
}

bool IsInf::convertImpl(String & out, IParser::Pos & pos)
{
    const auto fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;
    const auto arg = getArgument(fn_name, pos);
    out = fmt::format("toBool(isInfinite({}))", arg);
    return true;
}

bool Exp::convertImpl(String & out, IParser::Pos & pos)
{
    return directMapping(out, pos, "exp");
}
}
