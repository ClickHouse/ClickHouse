#pragma once

#include <base/scope_guard.h>

#include <Common/Exception.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/UnionNode.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

/** Same as InDepthQueryTreeVisitor (but has a different interface) and additionally keeps track of current scope context.
  * This can be useful if your visitor has special logic that depends on current scope context.
  *
  * To specify behavior of the visitor you can implement following methods in derived class:
  * 1. needChildVisit – This methods allows to skip subtree.
  * 2. enterImpl – This method is called before children are processed.
  * 3. leaveImpl – This method is called after children are processed.
  */
template <typename Derived>
class InDepthQueryTreeVisitorWithContext
{
public:
    using VisitQueryTreeNodeType = QueryTreeNodePtr;

    explicit InDepthQueryTreeVisitorWithContext(ContextPtr context, size_t initial_subquery_depth = 0)
        : current_context(std::move(context))
        , subquery_depth(initial_subquery_depth)
    {}

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

    size_t getSubqueryDepth() const
    {
        return subquery_depth;
    }

    void visit(VisitQueryTreeNodeType & query_tree_node)
    {
        auto current_scope_context_ptr = current_context;
        SCOPE_EXIT(
            current_context = std::move(current_scope_context_ptr);
            --subquery_depth;
        );

        if (auto * query_node = query_tree_node->template as<QueryNode>())
            current_context = query_node->getContext();
        else if (auto * union_node = query_tree_node->template as<UnionNode>())
            current_context = union_node->getContext();

        ++subquery_depth;

        getDerived().enterImpl(query_tree_node);

        visitChildren(query_tree_node);

        getDerived().leaveImpl(query_tree_node);
    }

    void enterImpl(VisitQueryTreeNodeType & node [[maybe_unused]])
    {}

    void leaveImpl(VisitQueryTreeNodeType & node [[maybe_unused]])
    {}
private:
    Derived & getDerived()
    {
        return *static_cast<Derived *>(this);
    }

    const Derived & getDerived() const
    {
        return *static_cast<Derived *>(this);
    }

    void visitChildIfNeeded(
        VisitQueryTreeNodeType & parent,
        VisitQueryTreeNodeType & child)
    {
        bool need_visit_child = getDerived().needChildVisit(parent, child);
        if (!need_visit_child)
            return;

        // We do not visit ListNode with arguments of TableFunctionNode directly, because
        // we need to know which arguments are in the unresolved state.
        // It must be safe because we do not modify table function arguments list in any visitor.
        if (auto * table_function_node = parent->as<TableFunctionNode>())
        {
            if (child != table_function_node->getArgumentsNode())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TableFunctionNode is expected to have only one child node");

            const auto & unresolved_indexes = table_function_node->getUnresolvedArgumentIndexes();

            size_t index = 0;
            for (auto & argument : table_function_node->getArguments())
            {
                if (std::find(unresolved_indexes.begin(),
                              unresolved_indexes.end(),
                              index) == unresolved_indexes.end())
                    visit(argument);
                ++index;
            }
            return;
        }
        visit(child);
    }

    void visitChildren(VisitQueryTreeNodeType & expression)
    {
        for (auto & child : expression->getChildren())
        {
            if (child)
                visitChildIfNeeded(expression, child);
        }
    }

    ContextPtr current_context;
    size_t subquery_depth = 0;
};

}
