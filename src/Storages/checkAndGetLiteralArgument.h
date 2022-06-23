#pragma once

#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename T>
T checkAndGetLiteralArgument(const ASTPtr & arg, const String & arg_name);

template <typename T>
T checkAndGetLiteralArgument(const ASTLiteral & arg, const String & arg_name);



}
