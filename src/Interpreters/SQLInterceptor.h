#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{
class ASTAlterQuery;
class ASTDropQuery;
class ASTOptimizeQuery;

class SQLInterceptor
{
public:
    static void intercept(const ASTPtr & ast_ptr, ContextPtr context);
    static void alterIntercept(const ASTAlterQuery & alter, ContextPtr  context);
    static void dropIntercept(const ASTDropQuery & drop, ContextPtr context);
    static void optimizeIntercept(const ASTOptimizeQuery & optimize, ContextPtr context);
};
}
