#include <Analyzer/Passes/ConvertOrHasAnyChainPass.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include <Core/Field.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/logical.h>

#include <Interpreters/Context.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/UnionNode.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool optimize_or_has_any_chain;
}

namespace
{

class ConvertOrHasAnyChainVisitor : public InDepthQueryTreeVisitorWithContext<ConvertOrHasAnyChainVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertOrHasAnyChainVisitor>;
    using Base::Base;

    explicit ConvertOrHasAnyChainVisitor(FunctionOverloadResolverPtr or_function_resolver_, ContextPtr context)
        : Base(std::move(context))
        , or_function_resolver(std::move(or_function_resolver_))
    {
    }

    bool needChildVisit(VisitQueryTreeNodeType &, VisitQueryTreeNodeType &)
    {
        const auto & settings = getSettings();
        return settings[Setting::optimize_or_has_any_chain];
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "or")
            return;

        QueryTreeNodes unique_elems;
        QueryTreeNodePtrWithHashMap<Array> node_to_patterns;
        FunctionNodes hasany_functions;

        for (auto & argument : function_node->getArguments())
        {
            unique_elems.push_back(argument);

            auto * argument_function = argument->as<FunctionNode>();
            // Should be hasAny - otherwise not interesting
            if (!argument_function || argument_function->getFunctionName() != "hasAny")
                continue;

            const auto & hasany_arguments = argument_function->getArguments().getNodes();
            if (hasany_arguments.size() != 2)
                continue;

            const auto & hasany_first_argument = hasany_arguments[0];
            const auto * pattern = hasany_arguments[1]->as<ConstantNode>();
            if (!pattern)
                continue;

            Array pattern_array;
            if (!pattern->getValue().tryGet(pattern_array))
                continue;

            unique_elems.pop_back();

            auto it = node_to_patterns.find(hasany_first_argument);
            if (it == std::end(node_to_patterns))
            {
                it = node_to_patterns.insert({hasany_first_argument, Array{}}).first;

                // Create new hasAny function with array argument
                auto new_hasany_function = std::make_shared<FunctionNode>("hasAny");
                new_hasany_function->getArguments().getNodes().push_back(hasany_first_argument);
                hasany_functions.push_back(new_hasany_function);

                unique_elems.push_back(std::move(new_hasany_function));
            }

            // Merge constant arrays
            auto & existing_array = it->second;
            existing_array.insert(std::end(existing_array), std::begin(pattern_array), std::end(pattern_array));
        }

        // Add merged constant arrays to hasAny function arguments
        for (auto & hasany_function : hasany_functions)
        {
            auto & arguments = hasany_function->getArguments().getNodes();
            auto & patterns = node_to_patterns.at(arguments[0]);
            // Deduplicate patterns:
            std::sort(std::begin(patterns), std::end(patterns));
            patterns.erase(std::unique(std::begin(patterns), std::end(patterns)), std::end(patterns));
            arguments.push_back(std::make_shared<ConstantNode>(Field{std::move(patterns)}));
            hasany_function->resolveAsFunction(getHasAnyFunctionResolver());
        }

        // OR must have at least two arguments
        if (unique_elems.size() == 1)
            unique_elems.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(0)));


        function_node->getArguments().getNodes() = std::move(unique_elems);
        function_node->resolveAsFunction(or_function_resolver);
    }

private:
    FunctionOverloadResolverPtr const & getHasAnyFunctionResolver()
    {
        if (!hasany_function_resolver)
            hasany_function_resolver = FunctionFactory::instance().get("hasAny", getContext());
        return hasany_function_resolver;
    }

    using FunctionNodes = std::vector<std::shared_ptr<FunctionNode>>;
    const FunctionOverloadResolverPtr or_function_resolver;
    mutable FunctionOverloadResolverPtr hasany_function_resolver;
};

}

void ConvertOrHasAnyChainPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto or_function_resolver = createInternalFunctionOrOverloadResolver();

    ConvertOrHasAnyChainVisitor visitor(std::move(or_function_resolver), std::move(context));
    visitor.visit(query_tree_node);
}

}
