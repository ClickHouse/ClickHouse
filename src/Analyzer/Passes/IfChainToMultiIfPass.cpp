#include <Analyzer/Passes/IfChainToMultiIfPass.h>


#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Core/Settings.h>
#include <Functions/multiIf.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_execute_multiif_columnar;
    extern const SettingsBool optimize_if_chain_to_multiif;
    extern const SettingsBool use_variant_as_common_type;
    extern const SettingsBool allow_lossy_numeric_supertype;
}

namespace
{

class IfChainToMultiIfPassVisitor : public InDepthQueryTreeVisitorWithContext<IfChainToMultiIfPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<IfChainToMultiIfPassVisitor>;
    using Base::Base;

    explicit IfChainToMultiIfPassVisitor(FunctionOverloadResolverPtr multi_if_function_ptr_, ContextPtr context)
        : Base(std::move(context))
        , multi_if_function_ptr(std::move(multi_if_function_ptr_))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_if_chain_to_multiif])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "if" || function_node->getArguments().getNodes().size() != 3)
            return;

        std::vector<QueryTreeNodePtr> multi_if_arguments;

        auto & function_node_arguments = function_node->getArguments().getNodes();
        multi_if_arguments.insert(multi_if_arguments.end(), function_node_arguments.begin(), function_node_arguments.end());

        QueryTreeNodePtr if_chain_node = multi_if_arguments.back();

        while (true)
        {
            /// Check if last `multiIf` argument is `if` function
            auto * if_chain_function_node = if_chain_node->as<FunctionNode>();
            if (!if_chain_function_node || if_chain_function_node->getFunctionName() != "if" || if_chain_function_node->getArguments().getNodes().size() != 3)
                break;

            /// Replace last `multiIf` argument with `if` function arguments

            multi_if_arguments.pop_back();

            auto & if_chain_function_node_arguments = if_chain_function_node->getArguments().getNodes();
            multi_if_arguments.insert(multi_if_arguments.end(), if_chain_function_node_arguments.begin(), if_chain_function_node_arguments.end());

            /// Use last `multiIf` argument for next check
            if_chain_node = multi_if_arguments.back();
        }

        /// Do not replace `if` with 3 arguments to `multiIf`
        if (multi_if_arguments.size() <= 3)
            return;

        auto multi_if_function = std::make_shared<FunctionNode>("multiIf");
        multi_if_function->getArguments().getNodes() = std::move(multi_if_arguments);
        multi_if_function->resolveAsFunction(multi_if_function_ptr->build(multi_if_function->getArgumentColumns()));

        /// Ignore if returned type changed.
        /// Example : SELECT now64(if(Null, NULL, if(Null, nan, toFloat64(number))), Null) FROM numbers(2)
        if (!multi_if_function->getResultType()->equals(*function_node->getResultType()))
            return;

        node = std::move(multi_if_function);
    }

private:
    FunctionOverloadResolverPtr multi_if_function_ptr;
};

}

void IfChainToMultiIfPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    /// Build the synthesized `multiIf` without the `optimize_if_transform_const_strings_to_lowcardinality`
    /// optimization so its result type matches the `if` chain it replaces. The outer `if` of a chain has
    /// a nested `if` (not a constant) as its else-branch, so - because `FunctionIf` strips
    /// `LowCardinality` from its arguments before inferring the return type - it always resolves to a
    /// plain (non-`LowCardinality`) `String`. If the `multiIf` were built with the optimization it would
    /// instead see all the constant string leaves and resolve to `LowCardinality(String)`; the type would
    /// then differ from the original `if`, the type-equality guard in the visitor would reject the rewrite,
    /// and enabling the setting would silently disable `optimize_if_chain_to_multiif` for exactly the
    /// constant-string chains this optimization targets.
    auto multi_if_function_ptr = createInternalMultiIfOverloadResolver(
        settings[Setting::allow_execute_multiif_columnar],
        settings[Setting::use_variant_as_common_type],
        settings[Setting::allow_lossy_numeric_supertype],
        /*optimize_if_transform_const_strings_to_lowcardinality=*/false);
    IfChainToMultiIfPassVisitor visitor(std::move(multi_if_function_ptr), std::move(context));
    visitor.visit(query_tree_node);
}
}
