#pragma once

#include <typeinfo>
#include <vector>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Parsers/DumpASTNode.h>


namespace DB
{

/// Visits AST tree in depth, call functions for nodes according to Matcher type data.
/// You need to define Data, visit() and needChildVisit() in Matcher class.
template <typename Matcher, bool _top_to_bottom, bool need_child_accept_data = false, typename T = ASTPtr>
class InDepthNodeVisitor
{
public:
    using Data = typename Matcher::Data;

    explicit InDepthNodeVisitor(Data & data_, WriteBuffer * ostr_ = nullptr)
    :   data(data_),
        visit_depth(0),
        ostr(ostr_)
    {}

    void visit(T & ast)
    {
        if (ostr)
            visitImpl</* with_dump= */ true>(ast);
        else
            visitImpl</* with_dump= */ false>(ast);
    }

private:
    Data & data;
    size_t visit_depth;
    WriteBuffer * ostr;

    template <bool with_dump>
    void visitImpl(T & ast)
    {
        checkStackSize();
        if constexpr (with_dump)
        {
            DumpASTNode dump(*ast, ostr, visit_depth, typeid(Matcher).name());
            visitImplMain</* with_dump= */ true>(ast);
        }
        else
        {
            visitImplMain</* with_dump= */ false>(ast);
        }
    }

    template <bool with_dump>
    void visitImplMain(T & ast)
    {
        if constexpr (!_top_to_bottom)
            visitChildren<with_dump>(ast);

        doVisit(ast);

        if constexpr (_top_to_bottom)
            visitChildren<with_dump>(ast);
    }

    void doVisit(T & ast)
    {
        try
        {
            Matcher::visit(ast, data);
        }
        catch (Exception & e)
        {
            e.addMessage("While processing {}", ast->formatForErrorMessage());
            throw;
        }
    }

    template <bool with_dump>
    void visitChildren(T & ast)
    {
        for (auto & child : ast->children)
        {
            bool need_visit_child = false;
            if constexpr (need_child_accept_data)
                need_visit_child = Matcher::needChildVisit(ast, child, data);
            else
                need_visit_child = Matcher::needChildVisit(ast, child);

            if (need_visit_child)
                visitImpl<with_dump>(child);
        }
    }
};

template <typename Matcher, bool top_to_bottom, bool need_child_accept_data = false>
using ConstInDepthNodeVisitor = InDepthNodeVisitor<Matcher, top_to_bottom, need_child_accept_data, const ASTPtr>;

struct NeedChild
{
    using Condition = bool (*)(const ASTPtr & node, const ASTPtr & child);

    static bool all(const ASTPtr &, const ASTPtr &) { return true; }
    static bool none(const ASTPtr &, const ASTPtr &) { return false; }
};

/// Simple matcher for one node type. Use need_child function for complex traversal logic.
template <typename DataImpl, NeedChild::Condition need_child = NeedChild::all, typename T = ASTPtr>
class OneTypeMatcher
{
public:
    using Data = DataImpl;
    using TypeToVisit = typename Data::TypeToVisit;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child) { return need_child(node, child); }

    static void visit(T & ast, Data & data)
    {
        if (auto * t = typeid_cast<TypeToVisit *>(ast.get()))
            data.visit(*t, ast);
    }
};

template <typename Data, NeedChild::Condition need_child = NeedChild::all>
using ConstOneTypeMatcher = OneTypeMatcher<Data, need_child, const ASTPtr>;

template <typename Visitor, typename T = ASTPtr>
struct InDepthNodeVisitorWithChildInfo : Visitor
{
    using ChildInfo = typename Visitor::ChildInfo;

    ChildInfo visit(T & ast, const T & parent = {})
    {
        ChildInfo all_children_info;
        for (auto & child : ast->children)
        {
            if (Visitor::needVisitChild(ast, child))
            {
                ChildInfo child_info = visit(child, ast);
                all_children_info.update(child_info);
            }
        }

        try
        {
            return Visitor::visitNode(ast, parent, all_children_info);
        }
        catch (Exception & e)
        {
            e.addMessage("While processing {}", ast->formatForErrorMessage());
            throw;
        }
    }
};

}
