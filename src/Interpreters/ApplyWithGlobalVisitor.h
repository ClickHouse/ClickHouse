#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Pull out the WITH clause from the first SELECT of a UNION query and wrap the
/// UNION in an outer `SELECT * FROM (UNION)` that carries the WITH clause.
/// This lets every branch of the UNION resolve CTEs through the parent scope.
class ApplyWithGlobalVisitor
{
public:
    static void visit(ASTPtr & ast);
};

}
