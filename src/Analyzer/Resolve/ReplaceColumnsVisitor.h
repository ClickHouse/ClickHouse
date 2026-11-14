#pragma once

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeFunction.h>
#include <Functions/FunctionFactory.h>
#include <Common/assert_cast.h>
#include <fmt/ranges.h>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Used to replace columns that changed type because of JOIN to their original type
class ReplaceColumnsVisitor : public InDepthQueryTreeVisitor<ReplaceColumnsVisitor>
{
public:
    explicit ReplaceColumnsVisitor(const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map_, const ContextPtr & context_)
        : replacement_map(replacement_map_)
        , context(context_)
    {}

    /// Apply replacement transitively, because column may change it's type twice, one to have a supertype and then because of `joun_use_nulls`
    static QueryTreeNodePtr findTransitiveReplacement(QueryTreeNodePtr node, const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map_)
    {
        auto it = replacement_map_.find(node);
        QueryTreeNodePtr result_node = nullptr;
        for (; it != replacement_map_.end(); it = replacement_map_.find(result_node))
        {
            if (result_node && result_node->isEqual(*it->second))
            {
                Strings map_dump;
                for (const auto & [k, v]: replacement_map_)
                    map_dump.push_back(fmt::format("{} -> {} (is_equals: {}, is_same: {})",
                        k.node->dumpTree(), v->dumpTree(), k.node->isEqual(*v), k.node == v));
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Infinite loop in query tree replacement map: {}", fmt::join(map_dump, "; "));
            }
            chassert(it->second);

            result_node = it->second;
        }
        return result_node;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto replacement_node = findTransitiveReplacement(node, replacement_map))
            node = replacement_node;

        if (auto * function_node = node->as<FunctionNode>(); function_node && function_node->isResolved())
        {
            visitLambdaArguments(*function_node);
            rerunFunctionResolve(function_node, context);
        }
        else if (auto * column_node = node->as<ColumnNode>())
        {
            /// A lambda parameter has the type its lambda is bound with.
            auto column_source = column_node->getColumnSourceOrNull();
            if (const auto * lambda_arguments = column_source ? column_source->as<LambdaArgumentsNode>() : nullptr)
            {
                const auto & names = lambda_arguments->getNames();
                const auto & types = lambda_arguments->getTypes();
                auto it = std::ranges::find(names, column_node->getColumnName());
                if (it != names.end() && types.size() == names.size())
                    column_node->setColumnType(types[it - names.begin()]);
            }
        }
    }

    /// We want to re-run resolve for function _after_ its arguments are replaced
    bool shouldTraverseTopToBottom() const { return false; }

    bool needChildVisit(QueryTreeNodePtr & /* parent */, QueryTreeNodePtr & child)
    {
        /// Visit only expressions, but not subqueries. A lambda is visited by its function.
        return child->getNodeType() == QueryTreeNodeType::IDENTIFIER
            || child->getNodeType() == QueryTreeNodeType::LIST
            || child->getNodeType() == QueryTreeNodeType::FUNCTION
            || child->getNodeType() == QueryTreeNodeType::COLUMN;
    }

private:
    /// A higher-order function derives the parameter types of its lambdas from its other arguments,
    /// so the lambdas are bound again before their bodies are visited.
    void visitLambdaArguments(FunctionNode & function_node)
    {
        auto & arguments = function_node.getArguments().getNodes();
        if (!function_node.isOrdinaryFunction()
            || std::ranges::none_of(arguments, [](const auto & argument) { return argument->getNodeType() == QueryTreeNodeType::LAMBDA; }))
            return;

        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
        {
            if (const auto * lambda_node = argument->as<LambdaNode>())
                argument_types.push_back(std::make_shared<DataTypeFunction>(DataTypes(lambda_node->getArguments().getNames().size(), nullptr), nullptr));
            else
                argument_types.push_back(argument->getResultType());
        }

        FunctionFactory::instance().get(function_node.getFunctionName(), context)->getLambdaArgumentTypes(argument_types);

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            auto * lambda_node = arguments[i]->as<LambdaNode>();
            if (!lambda_node)
                continue;

            const auto & lambda_argument_types = assert_cast<const DataTypeFunction &>(*argument_types[i]).getArgumentTypes();
            lambda_node->getArguments().resolve(lambda_argument_types);
            visit(arguments[i]);
            lambda_node->resolve(std::make_shared<DataTypeFunction>(lambda_argument_types, lambda_node->getExpression()->getResultType()));
        }
    }

    const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map;
    const ContextPtr & context;
};

}
