#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Builds the query `SELECT DISTINCT l.* FROM (arm_1) l SEMI|ANTI LEFT JOIN (arm_2) r ON l.c_i <=> r.c_i ...`
/// that is equivalent to the `INTERSECT DISTINCT` or `EXCEPT DISTINCT` union node, folding more than two arms
/// from the left. The null-safe comparison matches `NULL` with `NULL` like the set operation does.
/// The columns are converted to the union's result types like the set-operation step converts its inputs.
/// Returns nullptr when an arm has duplicate column names, which cannot be referenced from the join, or a column
/// has a `Dynamic` type, which cannot be a join key.
QueryTreeNodePtr rewriteIntersectOrExceptToJoin(const QueryTreeNodePtr & union_node, const ContextPtr & context);

}
