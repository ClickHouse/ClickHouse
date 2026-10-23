#pragma once

#include <Parsers/ASTFunction.h>
#include <string_view>


namespace DB
{

static inline bool isFunctionCast(std::string_view name) /// NOLINT
{
    return name == "CAST" || name == "_CAST";
}

static inline bool isFunctionCast(const ASTFunction * function) /// NOLINT
{
    return function && isFunctionCast(function->name);
}


}
