#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Rejects a multipart table path (`db.ns.t`) unless `allow_experimental_table_namespaces` is on
/// for the (sub)query that contains it.
class RejectTablePathsVisitor
{
public:
    static void visit(const ASTPtr & ast, const ContextPtr & context);
};

}
