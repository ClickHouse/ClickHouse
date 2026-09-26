#include <Analyzer/Passes/OptimizeLimitByInjectiveFunctionsPass.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool group_by_use_nulls;
extern const SettingsBool optimize_injective_functions_in_limit_by;
}

namespace
{

class OptimizeLimitByInjectiveFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeLimitByInjectiveFunctionsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeLimitByInjectiveFunctionsVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_injective_functions_in_limit_by])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasLimitBy())
            return;

        auto & limit_by = query->getLimitBy().getNodes();

        /// `LIMIT BY` is evaluated after aggregation, where only the `GROUP BY` keys and the
        /// aggregates exist. A key that is itself a `GROUP BY` key (`LIMIT BY c0 + 1` with
        /// `GROUP BY c0 + 1`) must therefore stay as it is: unwrapping it to `c0` would ask for a
        /// column that is gone once the rows are aggregated. `GROUP BY` has its own unwrapping pass,
        /// controlled by `optimize_injective_functions_in_group_by`, whose result is visible here.
        QueryTreeNodePtrWithHashSet group_by_keys;
        if (query->hasGroupBy())
        {
            /// `group_by_use_nulls` makes everything resolved after `GROUP BY` see the grouping keys
            /// promoted to `Nullable`: `QueryAnalyzer` replaces such an expression with a clone of the
            /// key converted to `Nullable` (see `registerNullableGroupByKeys`). Node comparison
            /// includes the result type, so the original key alone does not match the shape that
            /// arrives in `LIMIT BY`, and the unwrapping would walk past the key again. Register the
            /// converted shape as well. Only a `FunctionNode` needs it: `unwrapInjectiveFunctionsInKeys`
            /// keeps every other kind of node anyway.
            const bool group_by_use_nulls = getSettings()[Setting::group_by_use_nulls]
                && (query->isGroupByWithGroupingSets() || query->isGroupByWithRollup() || query->isGroupByWithCube());

            auto register_key = [&](const QueryTreeNodePtr & key)
            {
                group_by_keys.insert(key);

                if (group_by_use_nulls && key->as<FunctionNode>())
                {
                    auto nullable_key = key->clone();
                    nullable_key->convertToNullable();
                    group_by_keys.insert(std::move(nullable_key));
                }
            };

            if (query->isGroupByWithGroupingSets())
            {
                for (const auto & grouping_set : query->getGroupBy().getNodes())
                    for (const auto & key : grouping_set->as<ListNode &>().getNodes())
                        register_key(key);
            }
            else
            {
                for (const auto & key : query->getGroupBy().getNodes())
                    register_key(key);
            }
        }

        auto new_limit_by = unwrapInjectiveFunctionsInKeys(limit_by, false, query->hasGroupBy() ? &group_by_keys : nullptr);

        /// Atleast one key is needed for LIMIT BY.
        if (!new_limit_by.empty())
            limit_by = std::move(new_limit_by);
    }
};

}

void OptimizeLimitByInjectiveFunctionsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeLimitByInjectiveFunctionsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
