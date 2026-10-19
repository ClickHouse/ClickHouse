#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>

#include <map>

namespace DB
{

class ASTSelectWithUnionQuery;
class ASTSelectQuery;
class ASTSelectIntersectExceptQuery;
class ExpandedASTBudget;

/// Pull out the WITH statement from the first child of ASTSelectWithUnion query if any.
/// Throws `TOO_BIG_AST` when the copies of the WITH statement exceed `max_expanded_ast_elements` (zero means no limit).
class ApplyWithGlobalVisitor
{
public:
    static void visit(ASTPtr & ast, size_t max_expanded_ast_elements);

private:
    static void visit(ASTPtr & ast, ExpandedASTBudget & budget);
    static void visit(
        ASTSelectWithUnionQuery & selects,
        const std::map<String, ASTPtr> & exprs,
        const ASTPtr & with_expression_list,
        bool recursive_with,
        ExpandedASTBudget & budget);
    static void visit(
        ASTSelectQuery & select,
        const std::map<String, ASTPtr> & exprs,
        const ASTPtr & with_expression_list,
        bool recursive_with,
        ExpandedASTBudget & budget);
    static void visit(
        ASTSelectIntersectExceptQuery & select,
        const std::map<String, ASTPtr> & exprs,
        const ASTPtr & with_expression_list,
        bool recursive_with,
        ExpandedASTBudget & budget);
};

}
