#pragma once

#include <Common/typeid_cast.h>
#include <Parsers/DumpASTNode.h>
#include <unordered_map>

namespace DB
{

class ASTSelectWithUnionQuery;
class ASTSubquery;
struct ASTTableExpression;
struct ASTArrayJoin;

using Aliases = std::unordered_map<String, ASTPtr>;

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and fuction could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
class QueryAliasesVisitor
{
public:
    QueryAliasesVisitor(Aliases & aliases_, std::ostream * ostr_ = nullptr)
    :   aliases(aliases_),
        visit_depth(0),
        ostr(ostr_)
    {}

    void visit(const ASTPtr & ast) const;

private:
    Aliases & aliases;
    mutable size_t visit_depth;
    std::ostream * ostr;

    void visit(const ASTTableExpression &, const ASTPtr &) const {}
    void visit(const ASTSelectWithUnionQuery &, const ASTPtr &) const {}

    void visit(ASTSubquery & subquery, const ASTPtr & ast) const;
    void visit(const ASTArrayJoin &, const ASTPtr & ast) const;
    void visitOther(const ASTPtr & ast) const;
    void visitChildren(const ASTPtr & ast) const;

    template <typename T>
    bool tryVisit(const ASTPtr & ast) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            DumpASTNode dump(*ast, ostr, visit_depth, "getQueryAliases");
            visit(*t, ast);
            return true;
        }
        return false;
    }

    String wrongAliasMessage(const ASTPtr & ast, const String & alias) const;
};

}
