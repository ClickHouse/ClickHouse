#pragma once

#include <typeinfo>
#include <vector>
#include <Common/typeid_cast.h>
#include <Parsers/DumpASTNode.h>

namespace DB
{

/// Visits AST tree in depth, call functions for nodes according to Matcher type data.
/// You need to define Data, visit() and needChildVisit() in Matcher class.
template <typename Matcher, bool _top_to_bottom, typename T = ASTPtr>
class InDepthNodeVisitor
{
public:
    using Data = typename Matcher::Data;

    InDepthNodeVisitor(Data & data_, std::ostream * ostr_ = nullptr)
    :   data(data_),
        visit_depth(0),
        ostr(ostr_)
    {}

    void visit(T & ast)
    {
        DumpASTNode dump(*ast, ostr, visit_depth, typeid(Matcher).name());

        if constexpr (!_top_to_bottom)
            visitChildren(ast);

        Matcher::visit(ast, data);

        if constexpr (_top_to_bottom)
            visitChildren(ast);
    }

private:
    Data & data;
    size_t visit_depth;
    std::ostream * ostr;

    void visitChildren(T & ast)
    {
        for (auto & child : ast->children)
            if (Matcher::needChildVisit(ast, child))
                visit(child);
    }
};

template <typename Matcher, bool top_to_bottom>
using ConstInDepthNodeVisitor = InDepthNodeVisitor<Matcher, top_to_bottom, const ASTPtr>;

/// Simple matcher for one node type without complex traversal logic.
template <typename Data_, bool visit_children = true, typename T = ASTPtr>
class OneTypeMatcher
{
public:
    using Data = Data_;
    using TypeToVisit = typename Data::TypeToVisit;

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return visit_children; }

    static void visit(T & ast, Data & data)
    {
        if (auto * t = typeid_cast<TypeToVisit *>(ast.get()))
            data.visit(*t, ast);
    }
};

template <typename Data, bool visit_children = true>
using ConstOneTypeMatcher = OneTypeMatcher<Data, visit_children, const ASTPtr>;

}
