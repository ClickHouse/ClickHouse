#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite dictGet(‘dict’, ´dictField’ , tableField) = 'literal' into
/// tableField IN (SELECT groupArray(dictId) FROM dictionary('dict') WHERE dictField = 'literal')
class OptimizeDictGetEqualsMatcher
{
public:
    struct Data {};
    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using OptimizeDictGetEqualsToInVisitor = InDepthNodeVisitor<OptimizeDictGetEqualsMatcher, true>;

}
