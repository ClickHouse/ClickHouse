#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
    time_t getValidUntilFromAST(ASTPtr valid_until, ContextPtr context);
}
