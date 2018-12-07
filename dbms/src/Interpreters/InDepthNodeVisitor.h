#pragma once

#include <unordered_set>
#include <unordered_map>
#include <Parsers/DumpASTNode.h>

namespace DB
{

/// Visits AST tree in depth, call fucntions for nodes according to Matcher type data.
/// You need to define Data, label, visit() and needChildVisit() in Matcher class.
template <typename Matcher, bool _topToBottom>
class InDepthNodeVisitor
{
public:
    using Data = typename Matcher::Data;

    InDepthNodeVisitor(Data & data_, std::ostream * ostr_ = nullptr)
    :   data(data_),
        visit_depth(0),
        ostr(ostr_)
    {}

    void visit(ASTPtr & ast)
    {
        DumpASTNode dump(*ast, ostr, visit_depth, Matcher::label);

        if constexpr (!_topToBottom)
            visitChildren(ast);

        /// It operates with ASTPtr * cause we may want to rewrite ASTPtr in visit().
        std::vector<ASTPtr *> additional_nodes = Matcher::visit(ast, data);

        /// visit additional nodes (ex. only part of children)
        for (ASTPtr * node : additional_nodes)
            visit(*node);

        if constexpr (_topToBottom)
            visitChildren(ast);
    }

private:
    Data & data;
    size_t visit_depth;
    std::ostream * ostr;

    void visitChildren(ASTPtr & ast)
    {
        for (auto & child : ast->children)
            if (Matcher::needChildVisit(ast, child))
                visit(child);
    }
};

}
