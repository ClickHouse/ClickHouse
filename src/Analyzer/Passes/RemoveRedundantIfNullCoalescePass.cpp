#include <Analyzer/Passes/RemoveRedundantIfNullCoalescePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

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

class RemoveRedundantIfNullCoalesceVisitor : public InDepthQueryTreeVisitorWithContext<RemoveRedundantIfNullCoalesceVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RemoveRedundantIfNullCoalesceVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        /// This simplification exists to let KeyCondition / index analysis see the bare argument
        /// nested inside a key expression (e.g. sipHash64(ifNull(p, 0))). It is gated on the same
        /// compatibility knob as the top-level coalesce/ifNull key rewrite so that
        /// allow_key_condition_coalesce_rewrite = 0 (the pre-rewrite default recorded in
        /// SettingsChangesHistory) keeps ifNull/coalesce predicates opaque to the planner.
        if (!getSettings()[Setting::allow_key_condition_coalesce_rewrite])
            return;

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

        /// When the first argument cannot be NULL, ifNull/coalesce return it unchanged: the
        /// fallback arguments are never reached. The wrapper is then a pure identity and can be
        /// dropped so that key-expression matching sees the bare argument even when nested inside
        /// a larger expression (e.g. sipHash64(ifNull(p, 0))).
        if (canContainNull(*first_argument_type))
            return;

        /// The result type must match the argument type for a safe in-place replacement.
        if (!node->getResultType()->equals(*first_argument_type))
            return;

        /// ifNull/coalesce are NOT short-circuit (unlike if/multiIf): the fallback arguments are
        /// evaluated at execution even when the first argument is non-Nullable. Dropping them would
        /// silently discard their side effects, e.g. `ifNull(non_nullable, throwIf(1))` must still
        /// throw and `coalesce(x, sleepEachRow(1))` must still run. Only rewrite when every discarded
        /// fallback argument is a constant (provably side-effect-free), mirroring the guard in
        /// DictGetTupleElementPass for `tuple('ok', throwIf(1))`.
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (!arguments[i]->as<ConstantNode>())
                return;
        }

        node = first_argument;
    }
};

}

void RemoveRedundantIfNullCoalescePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RemoveRedundantIfNullCoalesceVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
