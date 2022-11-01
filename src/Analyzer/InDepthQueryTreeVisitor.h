#pragma once

#include <optional>
#include <utility>
#include <Common/SettingsChanges.h>
#include <Common/Exception.h>
#include <Core/Settings.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/QueryNode.h>


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

template <typename Impl>
class OptionalInDepthQueryTreeVisitor : Impl
{
    friend struct StateSwitcher;

    Settings settings;
    bool is_enabled = false;

    struct StateSwitcher
    {
        StateSwitcher(OptionalInDepthQueryTreeVisitor * visitor_, SettingsChanges rollback_changes_, bool value)
            : visitor(visitor_)
            , rollback_changes(std::move(rollback_changes_))
            , previous_value(visitor_->is_enabled)
        {
            visitor->is_enabled = value;
        }

        StateSwitcher(StateSwitcher &&) noexcept = default;

        ~StateSwitcher()
        {
            visitor->is_enabled = previous_value;
            for (const auto & change : rollback_changes)
                visitor->settings.set(change.name, change.value);
        }

        OptionalInDepthQueryTreeVisitor * visitor;
        SettingsChanges rollback_changes;
        bool previous_value;
    };

    std::optional<StateSwitcher> updateState(QueryTreeNodePtr & node)
    {
        auto * query = node->as<QueryNode>();
        if (!query)
            return {};

        SettingsChanges rollback_changes;
        for (const auto & change : query->getSettingsChanges())
        {
            rollback_changes.push_back(SettingChange{ change.name, settings.get(change.name) });
            settings.set(change.name, change.value);
        }
        return std::make_optional<StateSwitcher>(this, std::move(rollback_changes), Impl::isEnabled(settings));
    }

    void visitChildren(QueryTreeNodePtr & node)
    {
        for (auto & child : node->getChildren())
        {
            if (!child)
                continue;

            visit(child);
        }
    }

public:

    template <typename ...Args>
    explicit OptionalInDepthQueryTreeVisitor(const Settings & settings_, Args && ...args)
        : Impl(std::forward<Args>(args)...)
        , settings(settings_)
    {}

    bool needVisit(QueryTreeNodePtr & node)
    {
        return is_enabled && Impl::needVisit(node);
    }

    void visit(QueryTreeNodePtr & node)
    {
        auto switcher = updateState(node);

        if (needVisit(node))
            Impl::visitImpl(node);

        visitChildren(node);
    }
};

}
