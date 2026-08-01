#pragma once

#include <optional>
#include <string_view>

namespace DB
{

struct ExpressionOperatorPrettyInfo
{
    std::string_view symbol;
    int precedence;
};

/// Pretty-print metadata for expression operators (internal function name → infix symbol + precedence).
/// Implemented in `ExpressionListParsers.cpp`; built once from `ParserExpressionImpl` operator tables.
std::optional<ExpressionOperatorPrettyInfo> tryGetExpressionOperatorPrettyInfo(std::string_view function_name);

}
