#pragma once

#include <Parsers/IAST.h>
#include <map>

namespace DB
{

class ASTSelectWithUnionQuery;
class ASTSelectQuery;
class ASTSelectIntersectExceptQuery;

/// Pull out the WITH statement from the first child of ASTSelectWithUnion query if any.
class ApplyWithGlobalVisitor
{
public:
    static void visit(ASTPtr & ast);

private:
    static void visit(ASTSelectWithUnionQuery & selects, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list);
    static void visit(ASTSelectQuery & select, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list);
    static void visit(ASTSelectIntersectExceptQuery & select, const std::map<String, ASTPtr> & exprs, const ASTPtr & with_expression_list);
};

}
