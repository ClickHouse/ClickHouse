#include <Analyzer/Passes/ConvertOrLikeChainPass.h>

#include <memory>
#include <unordered_map>
#include <vector>

#include <Common/likePatternToRegexp.h>

#include <Core/Field.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/multiMatchAny.h>
#include <Functions/logical.h>

#include <Interpreters/Context.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_hyperscan;
    extern const SettingsUInt64 max_hyperscan_regexp_length;
    extern const SettingsUInt64 max_hyperscan_regexp_total_length;
    extern const SettingsBool reject_expensive_hyperscan_regexps;
    extern const SettingsBool optimize_or_like_chain;
}

namespace
{

class ConvertOrLikeChainVisitor : public InDepthQueryTreeVisitorWithContext<ConvertOrLikeChainVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertOrLikeChainVisitor>;
    using Base::Base;

    explicit ConvertOrLikeChainVisitor(FunctionOverloadResolverPtr or_function_resolver_,
        FunctionOverloadResolverPtr match_function_resolver_,
        ContextPtr context)
        : Base(std::move(context))
        , or_function_resolver(std::move(or_function_resolver_))
        , match_function_resolver(std::move(match_function_resolver_))
    {}

    bool needChildVisit(VisitQueryTreeNodeType &, VisitQueryTreeNodeType &)
    {
        const auto & settings = getSettings();

        return settings[Setting::optimize_or_like_chain] && settings[Setting::allow_hyperscan] && settings[Setting::max_hyperscan_regexp_length] == 0
            && settings[Setting::max_hyperscan_regexp_total_length] == 0;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "or")
            return;

        QueryTreeNodes unique_elems;

        QueryTreeNodePtrWithHashMap<Array> node_to_patterns;
        FunctionNodes match_functions;

        for (auto & argument : function_node->getArguments())
        {
            unique_elems.push_back(argument);

            auto * argument_function = argument->as<FunctionNode>();
            if (!argument_function)
                continue;

            const bool is_like  = argument_function->getFunctionName() == "like";
            const bool is_ilike = argument_function->getFunctionName() == "ilike";

            /// Not {i}like -> bail out.
            if (!is_like && !is_ilike)
                continue;

            const auto & like_arguments = argument_function->getArguments().getNodes();
            if (like_arguments.size() != 2)
                continue;

            const auto & like_first_argument = like_arguments[0];
            const auto * pattern = like_arguments[1]->as<ConstantNode>();
            if (!pattern || !isString(pattern->getResultType()))
                continue;

            auto regexp = likePatternToRegexp(pattern->getValue().safeGet<String>());
            /// Case insensitive. Works with UTF-8 as well.
            if (is_ilike)
                regexp = "(?i)" + regexp;

            unique_elems.pop_back();

            auto it = node_to_patterns.find(like_first_argument);
            if (it == node_to_patterns.end())
            {
                it = node_to_patterns.insert({like_first_argument, Array{}}).first;

                /// The second argument will be added when all patterns are known.
                auto match_function = std::make_shared<FunctionNode>("multiMatchAny");
                match_function->getArguments().getNodes().push_back(like_first_argument);
                match_functions.push_back(match_function);

                unique_elems.push_back(std::move(match_function));
            }

            it->second.push_back(regexp);
        }

        /// Add all the patterns into the function arguments lists.
        for (auto & match_function : match_functions)
        {
            auto & arguments = match_function->getArguments().getNodes();
            auto & patterns = node_to_patterns.at(arguments[0]);
            arguments.push_back(std::make_shared<ConstantNode>(Field{std::move(patterns)}));
            match_function->resolveAsFunction(match_function_resolver);
        }

        /// OR must have at least two arguments.
        if (unique_elems.size() == 1)
            unique_elems.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(0)));

        function_node->getArguments().getNodes() = std::move(unique_elems);
        function_node->resolveAsFunction(or_function_resolver);
    }
private:
    using FunctionNodes = std::vector<std::shared_ptr<FunctionNode>>;
    const FunctionOverloadResolverPtr or_function_resolver;
    const FunctionOverloadResolverPtr match_function_resolver;
};

}

void ConvertOrLikeChainPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    auto match_function_resolver = createInternalMultiMatchAnyOverloadResolver(
        settings[Setting::allow_hyperscan],
        settings[Setting::max_hyperscan_regexp_length],
        settings[Setting::max_hyperscan_regexp_total_length],
        settings[Setting::reject_expensive_hyperscan_regexps]);
    auto or_function_resolver = createInternalFunctionOrOverloadResolver();

    ConvertOrLikeChainVisitor visitor(std::move(or_function_resolver), std::move(match_function_resolver), std::move(context));
    visitor.visit(query_tree_node);
}

}
