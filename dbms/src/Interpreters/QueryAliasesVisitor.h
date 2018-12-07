#pragma once

#include <unordered_map>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTSelectWithUnionQuery;
class ASTSubquery;
struct ASTTableExpression;
struct ASTArrayJoin;

using Aliases = std::unordered_map<String, ASTPtr>;

/// Visits AST node to collect aliases.
class QueryAliasesMatcher
{
public:
    struct Data
    {
        Aliases & aliases;
    };

    static constexpr const char * label = "QueryAliases";

    static std::vector<ASTPtr> visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

private:
    static std::vector<ASTPtr> visit(ASTSubquery & subquery, const ASTPtr & ast, Data & data);
    static std::vector<ASTPtr> visit(const ASTArrayJoin &, const ASTPtr & ast, Data & data);
    static void visitOther(const ASTPtr & ast, Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using QueryAliasesVisitor = InDepthNodeVisitor<QueryAliasesMatcher, false>;

}
