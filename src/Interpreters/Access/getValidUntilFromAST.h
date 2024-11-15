#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
    time_t getValidUntilFromAST(ASTPtr, ContextPtr);
    time_t getNotBeforeFromAST(ASTPtr, ContextPtr);
}
