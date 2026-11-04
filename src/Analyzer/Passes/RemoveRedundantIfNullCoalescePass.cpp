#include <Analyzer/Passes/RemoveRedundantIfNullCoalescePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_key_condition_coalesce_rewrite;
}

namespace
{

class RemoveRedundantIfNullCoalesceVisitor : public InDepthQueryTreeVisitor<RemoveRedundantIfNullCoalesceVisitor>
{
public:
    bool shouldTraverseTopToBottom() const { return false; }

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        return !isSubqueryNodeType(child->getNodeType());
    }

    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const auto & function_name = function_node->getFunctionName();
        if (function_name != "ifNull" && function_name != "coalesce")
            return;

        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.empty())
            return;

        const auto & first_argument = arguments[0];
        const auto & first_argument_type = first_argument->getResultType();

        /// With a first argument that cannot be NULL, `ifNull` and `coalesce` return it unchanged.
        if (canContainNull(*first_argument_type))
            return;

        if (!node->getResultType()->equals(*first_argument_type))
            return;

        /// `ifNull` and `coalesce` are not short-circuit: every argument is evaluated, so dropping a
        /// non-constant one would lose its side effects (e.g. `throwIf`).
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (!arguments[i]->as<ConstantNode>())
                return;
        }

        node = first_argument;
    }
};

/// Only the filters: elsewhere two distinct grouping keys could become equal, or a key could stop matching
/// its `group_by_use_nulls` copy in the projection.
class QueryFiltersVisitor : public InDepthQueryTreeVisitorWithContext<QueryFiltersVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<QueryFiltersVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node || !getSettings()[Setting::allow_key_condition_coalesce_rewrite])
            return;

        if (query_node->hasPrewhere())
            RemoveRedundantIfNullCoalesceVisitor().visit(query_node->getPrewhere());
        if (query_node->hasWhere())
            RemoveRedundantIfNullCoalesceVisitor().visit(query_node->getWhere());
    }
};

}

void RemoveRedundantIfNullCoalescePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    QueryFiltersVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
