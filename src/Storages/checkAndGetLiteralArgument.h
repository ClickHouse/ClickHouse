#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTLiteral;

template <typename T>
T checkAndGetLiteralArgument(const ASTPtr & arg, const String & arg_name);

template <typename T>
T checkAndGetLiteralArgument(const ASTLiteral & arg, const String & arg_name);

}
