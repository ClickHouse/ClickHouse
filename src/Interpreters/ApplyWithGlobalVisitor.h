#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Context_fwd.h>
#include <map>

namespace DB
{

class ASTSelectWithUnionQuery;
class ASTSelectQuery;
class ASTSelectIntersectExceptQuery;

/// Pull out the WITH statement from the first child of ASTSelectWithUnion query if any.
class ApplyWithGlobalVisitor : public WithContext
{
public:
    ApplyWithGlobalVisitor(const ContextPtr & context_) : WithContext(context_) {}
    void visit(ASTPtr & ast);

private:
    void visit(ASTSelectWithUnionQuery & selects, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list);
    void visit(ASTSelectQuery & select, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list);
    void visit(ASTSelectIntersectExceptQuery & select, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list);
};

}
