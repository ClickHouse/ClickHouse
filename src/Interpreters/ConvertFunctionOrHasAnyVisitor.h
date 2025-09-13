#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

/// Replaces all the hasAny(arr, arr1) OR hasAny(arr, arr2) OR ... with hasAny(arr, arr1 + arr2 + ...).
class ConvertFunctionOrHasAnyData
{
public:
    using TypeToVisit = ASTFunction;

    static void visit(ASTFunction & function, ASTPtr & ast);
};

using ConvertFunctionOrHasAnyVisitor = InDepthNodeVisitor<OneTypeMatcher<ConvertFunctionOrHasAnyData>, true>;

}
