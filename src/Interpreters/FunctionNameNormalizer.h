#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

struct FunctionNameNormalizer
{
    static void visit(IAST *);
};

}
