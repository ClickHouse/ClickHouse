#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

template <typename ValueType>
static inline ValueType safeGetFromASTLiteral(const ASTPtr & ast)
{
    return ast->as<ASTLiteral>()->value.safeGet<ValueType>();
}

static inline bool isFunctionCast(const ASTFunction * function)
{
    if (function)
        return function->name == "CAST" || function->name == "_CAST";
    return false;
}


}
