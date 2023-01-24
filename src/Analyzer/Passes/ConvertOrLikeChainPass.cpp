#include <memory>
#include <unordered_map>
#include <vector>
#include <Analyzer/Passes/ConvertOrLikeChainPass.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/likePatternToRegexp.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace
{

class ConvertOrLikeChainVisitor : public InDepthQueryTreeVisitor<ConvertOrLikeChainVisitor>
{
    using FunctionNodes = std::vector<std::shared_ptr<FunctionNode>>;

    const FunctionOverloadResolverPtr match_function_ref;
    const FunctionOverloadResolverPtr or_function_resolver;
public:

    explicit ConvertOrLikeChainVisitor(ContextPtr context)
        : InDepthQueryTreeVisitor<ConvertOrLikeChainVisitor>()
        , match_function_ref(FunctionFactory::instance().get("multiMatchAny", context))
        , or_function_resolver(FunctionFactory::instance().get("or", context))
    {}

    static bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType &)
    {
        ContextPtr context;
        if (auto * query = parent->as<QueryNode>())
            context = query->getContext();
        else if (auto * union_node = parent->as<UnionNode>())
            context = union_node->getContext();
        if (context)
        {
            const auto & settings = context->getSettingsRef();
            return settings.optimize_or_like_chain
                && settings.allow_hyperscan
                && settings.max_hyperscan_regexp_length == 0
                && settings.max_hyperscan_regexp_total_length == 0;
        }
        return true;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "or")
            return;

        QueryTreeNodes unique_elems;

        QueryTreeNodePtrWithHashMap<Array> node_to_patterns;
        FunctionNodes match_functions;
        for (auto & arg : function_node->getArguments())
        {
            unique_elems.push_back(arg);

            auto * arg_func = arg->as<FunctionNode>();
            if (!arg_func)
                continue;

            const bool is_like  = arg_func->getFunctionName() == "like";
            const bool is_ilike = arg_func->getFunctionName() == "ilike";

            /// Not {i}like -> bail out.
            if (!is_like && !is_ilike)
                continue;

            const auto & like_arguments = arg_func->getArguments().getNodes();
            if (like_arguments.size() != 2)
                continue;

            auto identifier = like_arguments[0];
            auto * pattern = like_arguments[1]->as<ConstantNode>();
            if (!pattern || !isString(pattern->getResultType()))
                continue;

            auto regexp = likePatternToRegexp(pattern->getValue().get<String>());
            /// Case insensitive. Works with UTF-8 as well.
            if (is_ilike)
                regexp = "(?i)" + regexp;

            unique_elems.pop_back();
            auto it = node_to_patterns.find(identifier);
            if (it == node_to_patterns.end())
            {
                it = node_to_patterns.insert({identifier, Array{}}).first;
                /// The second argument will be added when all patterns are known.
                auto match_function = std::make_shared<FunctionNode>("multiMatchAny");
                match_function->getArguments().getNodes().push_back(identifier);

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
            match_function->resolveAsFunction(match_function_ref);
        }

        /// OR must have at least two arguments.
        if (unique_elems.size() == 1)
            unique_elems.push_back(std::make_shared<ConstantNode>(false));

        function_node->getArguments().getNodes() = std::move(unique_elems);
        function_node->resolveAsFunction(or_function_resolver);
    }
};

}

void ConvertOrLikeChainPass::run(QueryTreeNodePtr query_tree_node, ContextPtr  context)
{
    ConvertOrLikeChainVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
