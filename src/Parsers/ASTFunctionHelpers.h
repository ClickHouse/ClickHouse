#pragma once

#include <Parsers/ASTFunction.h>


namespace DB
{

static bool isFunctionCast(const ASTFunction * function)
{
    if (function)
        return function->name == "CAST" || function->name == "_CAST";
    return false;
}

}
