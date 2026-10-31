#pragma once

#include <Core/Names.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

/// Data for RenameColumnVisitor which traverse tree and rename all columns with
/// name column_name to rename_to
struct RenameColumnData
{
    String column_name;
    String rename_to;
    /// Compound identifiers bind a lambda parameter through their first part; aliases are lambda-scoped.
    bool new_analyzer_binding = false;
    /// Parameters of the enclosing lambdas.
    Names lambda_parameters = {};
};

struct RenameColumnMatcher
{
    using Data = RenameColumnData;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child, const Data & data);
    static void visit(ASTPtr & ast, Data & data);
};

using RenameColumnVisitor = InDepthNodeVisitor<RenameColumnMatcher, true, /*need_child_accept_data=*/ true>;

/// Finds the renames `RenameColumnVisitor` would do that change what a renamed identifier refers to.
class RenameColumnConflicts
{
public:
    explicit RenameColumnConflicts(const RenameColumnData & data_);
    /// The defaults of all columns: one scope, resolved by the analyzer. Each element: the expression and the text naming it.
    void addColumnDefaults(const std::vector<std::pair<ASTPtr, String>> & defaults);
    /// One expression resolved by `TreeRewriter`; a table TTL list is split into one scope per element TTL / WHERE expression.
    void add(const ASTPtr & ast, const String & where);
    /// Throws BAD_ARGUMENTS for the first conflict found.
    void throwIfAny() const;

private:
    void addScope(const std::vector<std::pair<ASTPtr, String>> & expressions, bool new_analyzer_binding);

    String column_name;
    String rename_to;
    String conflict;
};

}
