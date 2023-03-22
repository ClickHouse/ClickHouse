#pragma once

#include <base/scope_guard.h>

#include <Common/Exception.h>
#include <Core/Settings.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>

#include <Interpreters/Context.h>

namespace DB
{

/** Visitor that traverse query tree in depth.
  * Derived class must implement `visitImpl` method.
  * Additionally subclass can control if child need to be visited using `needChildVisit` method, by
  * default all node children are visited.
  * By default visitor traverse tree from top to bottom, if bottom to top traverse is required subclass
  * can override `shouldTraverseTopToBottom` method.
  *
  * Usage example:
  * class FunctionsVisitor : public InDepthQueryTreeVisitor<FunctionsVisitor>
  * {
  *     void visitImpl(VisitQueryTreeNodeType & query_tree_node)
  *     {
  *         if (query_tree_node->getNodeType() == QueryTreeNodeType::FUNCTION)
  *             processFunctionNode(query_tree_node);
  *     }
  * }
  */
template <typename Derived, bool const_visitor = false>
class InDepthQueryTreeVisitor
{
public:
    using VisitQueryTreeNodeType = std::conditional_t<const_visitor, const QueryTreeNodePtr, QueryTreeNodePtr>;

    /// Return true if visitor should traverse tree top to bottom, false otherwise
    bool shouldTraverseTopToBottom() const
    {
        return true;
    }

    /// Return true if visitor should visit child, false otherwise
    bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child [[maybe_unused]])
    {
        return true;
    }

    void visit(VisitQueryTreeNodeType & query_tree_node)
    {
        bool traverse_top_to_bottom = getDerived().shouldTraverseTopToBottom();
        if (!traverse_top_to_bottom)
            visitChildren(query_tree_node);

        getDerived().visitImpl(query_tree_node);

        if (traverse_top_to_bottom)
            visitChildren(query_tree_node);
    }

private:
    Derived & getDerived()
    {
        return *static_cast<Derived *>(this);
    }

    const Derived & getDerived() const
    {
        return *static_cast<Derived *>(this);
    }

    void visitChildren(VisitQueryTreeNodeType & expression)
    {
        for (auto & child : expression->getChildren())
        {
            if (!child)
                continue;

            bool need_visit_child = getDerived().needChildVisit(expression, child);

            if (need_visit_child)
                visit(child);
        }
    }
};

template <typename Derived>
using ConstInDepthQueryTreeVisitor = InDepthQueryTreeVisitor<Derived, true /*const_visitor*/>;

/** Same as InDepthQueryTreeVisitor and additionally keeps track of current scope context.
  * This can be useful if your visitor has special logic that depends on current scope context.
  */
template <typename Derived, bool const_visitor = false>
class InDepthQueryTreeVisitorWithContext
{
public:
    using VisitQueryTreeNodeType = std::conditional_t<const_visitor, const QueryTreeNodePtr, QueryTreeNodePtr>;

    explicit InDepthQueryTreeVisitorWithContext(ContextPtr context)
        : current_context(std::move(context))
    {}

    /// Return true if visitor should traverse tree top to bottom, false otherwise
    bool shouldTraverseTopToBottom() const
    {
        return true;
    }

    /// Return true if visitor should visit child, false otherwise
    bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child [[maybe_unused]])
    {
        return true;
    }

    const ContextPtr & getContext() const
    {
        return current_context;
    }

    const Settings & getSettings() const
    {
        return current_context->getSettingsRef();
    }

    void visit(VisitQueryTreeNodeType & query_tree_node)
    {
        auto current_scope_context_ptr = current_context;
        SCOPE_EXIT(
            current_context = std::move(current_scope_context_ptr);
        );

        if (auto * query_node = query_tree_node->template as<QueryNode>())
            current_context = query_node->getContext();
        else if (auto * union_node = query_tree_node->template as<UnionNode>())
            current_context = union_node->getContext();

        bool traverse_top_to_bottom = getDerived().shouldTraverseTopToBottom();
        if (!traverse_top_to_bottom)
            visitChildren(query_tree_node);

        getDerived().visitImpl(query_tree_node);

        if (traverse_top_to_bottom)
            visitChildren(query_tree_node);
    }
private:
    Derived & getDerived()
    {
        return *static_cast<Derived *>(this);
    }

    const Derived & getDerived() const
    {
        return *static_cast<Derived *>(this);
    }

    void visitChildren(VisitQueryTreeNodeType & expression)
    {
        for (auto & child : expression->getChildren())
        {
            if (!child)
                continue;

            bool need_visit_child = getDerived().needChildVisit(expression, child);

            if (need_visit_child)
                visit(child);
        }
    }

    ContextPtr current_context;
};

template <typename Derived>
using ConstInDepthQueryTreeVisitorWithContext = InDepthQueryTreeVisitorWithContext<Derived, true /*const_visitor*/>;

/** Visitor that use another visitor to visit node only if condition for visiting node is true.
  * For example, your visitor need to visit only query tree nodes or union nodes.
  *
  * Condition interface:
  * struct Condition
  * {
  *     bool operator()(VisitQueryTreeNodeType & node)
  *     {
  *         return shouldNestedVisitorVisitNode(node);
  *     }
  * }
  */
template <typename Visitor, typename Condition, bool const_visitor = false>
class InDepthQueryTreeConditionalVisitor : public InDepthQueryTreeVisitor<InDepthQueryTreeConditionalVisitor<Visitor, Condition, const_visitor>, const_visitor>
{
public:
    using Base = InDepthQueryTreeVisitor<InDepthQueryTreeConditionalVisitor<Visitor, Condition, const_visitor>, const_visitor>;
    using VisitQueryTreeNodeType = typename Base::VisitQueryTreeNodeType;

    explicit InDepthQueryTreeConditionalVisitor(Visitor & visitor_, Condition & condition_)
        : visitor(visitor_)
        , condition(condition_)
    {
    }

    bool shouldTraverseTopToBottom() const
    {
        return visitor.shouldTraverseTopToBottom();
    }

    void visitImpl(VisitQueryTreeNodeType & query_tree_node)
    {
        if (condition(query_tree_node))
            visitor.visit(query_tree_node);
    }

    Visitor & visitor;
    Condition & condition;
};

template <typename Visitor, typename Condition>
using ConstInDepthQueryTreeConditionalVisitor = InDepthQueryTreeConditionalVisitor<Visitor, Condition, true /*const_visitor*/>;

}
