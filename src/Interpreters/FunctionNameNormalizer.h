#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

struct FunctionNameNormalizer
{
    static void visit(IAST *);
};

}
