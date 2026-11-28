#include <Analyzer/Passes/ArrayExistsLikeToOrPass.h>


#include <Common/logger_useful.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/logical.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_rewrite_array_exists_like_to_or;
}

namespace
{

/// True if `node` is a use of the lambda variable: a column with its name whose source is the lambda.
bool isLambdaArgument(const QueryTreeNodePtr & node, const String & lambda_argument_name, const LambdaArgumentsNodePtr & lambda_arguments_node)
{
    const auto * column_node = node->as<ColumnNode>();
    return column_node && column_node->getColumnName() == lambda_argument_name
        && column_node->getColumnSourceOrNull() == lambda_arguments_node;
}

/// True if `node` or any node below it is a use of the lambda variable.
bool containsLambdaArgument(const QueryTreeNodePtr & node, const String & lambda_argument_name, const LambdaArgumentsNodePtr & lambda_arguments_node)
{
    if (isLambdaArgument(node, lambda_argument_name, lambda_arguments_node))
        return true;

    for (const auto & child : node->getChildren())
    {
        if (child && containsLambdaArgument(child, lambda_argument_name, lambda_arguments_node))
            return true;
    }

    return false;
}

bool isUnsafeToDuplicate(const QueryTreeNodePtr & node)
{
    if (!node)
        return false;

    /// Only check ORDINARY functions, WINDOW/AGGREGATE functions need
    /// to have their children checked instead, so we fall through to the recursive check.
    if (auto * function = node->as<FunctionNode>(); function && function->isOrdinaryFunction())
    {
        auto function_base = function->getFunctionOrThrow();
        if (!function_base->isDeterministicInScopeOfQuery() || function_base->isStateful() || function_base->hasObservableSideEffects())
            return true;
    }

    for (const auto & child : node->getChildren())
        if (isUnsafeToDuplicate(child))
            return true;

    return false;
}

class RewriteArrayExistsLikeToOrVisitor : public InDepthQueryTreeVisitorWithContext<RewriteArrayExistsLikeToOrVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteArrayExistsLikeToOrVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_array_exists_like_to_or])
            return;

        auto * array_exists_function_node = node->as<FunctionNode>();
        if (!array_exists_function_node || array_exists_function_node->getFunctionName() != "arrayExists")
            return;

        auto & array_exists_function_arguments_nodes = array_exists_function_node->getArguments().getNodes();
        if (array_exists_function_arguments_nodes.size() != 2)
            return;

        /// lambda function must be like: x -> haystack LIKE x
        auto * lambda_node = array_exists_function_arguments_nodes[0]->as<LambdaNode>();
        if (!lambda_node)
            return;

        const auto & lambda_argument_names = lambda_node->getArguments().getNames();
        if (lambda_argument_names.size() != 1)
            return;

        const auto & lambda_argument_name = lambda_argument_names[0];
        auto lambda_arguments_node = lambda_node->getArgumentsTyped();


        /// 3a: the lambda body must be one of the LIKE functions
        auto * like_function_node = lambda_node->getExpression()->as<FunctionNode>();
        if (!like_function_node)
            return;

        const auto & like_function_name = like_function_node->getFunctionName();
        if (like_function_name != "like" && like_function_name != "ilike")
            return;

        const auto & like_arguments_nodes = like_function_node->getArguments().getNodes();
        if (like_arguments_nodes.size() != 2)
            return;

        /// 3b: the pattern (2nd argument) must be the lambda variable
        if (!isLambdaArgument(like_arguments_nodes[1], lambda_argument_name, lambda_arguments_node))
            return;

        /// 3c: the text being searched (1st argument) must not use the lambda variable
        if (containsLambdaArgument(like_arguments_nodes[0], lambda_argument_name, lambda_arguments_node))
            return;

        /// 3d: the haystack is cloned once per pattern, so it must be safe to evaluate several times
        if (isUnsafeToDuplicate(like_arguments_nodes[0]))
            return;

        /// 4: the list must be a constant, non-empty array of strings (no NULLs)
        const auto * patterns_constant_node = array_exists_function_arguments_nodes[1]->as<ConstantNode>();
        if (!patterns_constant_node)
            return;

        const Field patterns_value = patterns_constant_node->getValue();
        if (patterns_value.getType() != Field::Types::Array)
            return;

        const auto & patterns = patterns_value.safeGet<Array>();
        if (patterns.empty())
            return;

        for (const auto & pattern : patterns)
        {
            if (pattern.getType() != Field::Types::String)
                return;
        }

        /// 5: build `or(like(haystack, 'p1'), like(haystack, 'p2'), ...)` and replace the `arrayExists` node with it
        const auto & haystack_node = like_arguments_nodes[0];
        auto like_function_resolver = FunctionFactory::instance().get(like_function_name, getContext());

        QueryTreeNodes like_nodes;
        like_nodes.reserve(patterns.size());
        for (const auto & pattern : patterns)
        {
            auto like_node = std::make_shared<FunctionNode>(like_function_name);
            like_node->getArguments().getNodes().push_back(haystack_node->clone());
            like_node->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(pattern));
            like_node->resolveAsFunction(like_function_resolver);
            like_nodes.push_back(std::move(like_node));
        }

        /// `or` needs at least two arguments, so a single pattern becomes just `like(haystack, 'p1')`
        QueryTreeNodePtr replacement_node;
        if (like_nodes.size() == 1)
        {
            replacement_node = std::move(like_nodes[0]);
        }
        else
        {
            auto or_node = std::make_shared<FunctionNode>("or");
            or_node->getArguments().getNodes() = std::move(like_nodes);
            or_node->resolveAsFunction(createInternalFunctionOrOverloadResolver());
            replacement_node = std::move(or_node);
        }

        /// The rest of the query was built around the result type of `arrayExists`, so skip the rewrite if it would change
        /// (for example, a `Nullable` haystack makes `like` return `Nullable(UInt8)`).
        if (!replacement_node->getResultType()->equals(*node->getResultType()))
            return;

        node = std::move(replacement_node);
    }
};

}

void RewriteArrayExistsLikeToOrPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteArrayExistsLikeToOrVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
