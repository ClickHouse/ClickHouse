#pragma once

#include <Parsers/DumpASTNode.h>
#include <unordered_map>

namespace DB
{

using Aliases = std::unordered_map<String, ASTPtr>;

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
class QueryAliasesVisitor
{
public:
    QueryAliasesVisitor(std::ostream * ostr_ = nullptr)
    :   visit_depth(0),
        ostr(ostr_)
    {}

    void visit(const ASTPtr & ast, Aliases & aliases, int ignore_levels = 0) const
    {
        getQueryAliases(ast, aliases, ignore_levels);
    }

private:
    static constexpr const char * visit_action = "addAlias";
    mutable size_t visit_depth;
    std::ostream * ostr;

    void getQueryAliases(const ASTPtr & ast, Aliases & aliases, int ignore_levels) const;
    void getNodeAlias(const ASTPtr & ast, Aliases & aliases, const DumpASTNode & dump) const;
};

}
