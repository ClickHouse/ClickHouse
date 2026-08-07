#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Rewrite function names to their canonical forms.
/// For example, rewrite (1) to (2)
/// (1) SELECT suM(1), AVG(2);
/// (2) SELECT sum(1), avg(2);
///
/// It's used to help projection query analysis matching function nodes by their canonical names.
/// See the comment of ActionsDAG::foldActionsByProjection for details.
struct FunctionNameNormalizer
{
    static void visit(IAST *);
};

}
