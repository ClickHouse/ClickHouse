#include <Analyzer/Passes/LogicalExpressionOptimizerPass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/HashUtils.h>

namespace DB
{

class LogicalExpressionOptimizerVisitor : public InDepthQueryTreeVisitorWithContext<LogicalExpressionOptimizerVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<LogicalExpressionOptimizerVisitor>;

    explicit LogicalExpressionOptimizerVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return;

        if (function_node->getFunctionName() == "or")
        {
            tryReplaceOrEqualsChainWithIn(node);
            return;
        }

        if (function_node->getFunctionName() == "and")
        {
            tryReplaceAndEqualsChainsWithConstant(node);
            return;
        }
    }
private:
    void tryReplaceAndEqualsChainsWithConstant(QueryTreeNodePtr & node)
    {
        auto & function_node = node->as<FunctionNode &>();
        assert(function_node.getFunctionName() == "and");

        if (function_node.getResultType()->isNullable())
            return;

        QueryTreeNodes and_operands;

        QueryTreeNodePtrWithHashMap<const ConstantNode *> node_to_constants;

        for (const auto & argument : function_node.getArguments())
        {
            auto * argument_function = argument->as<FunctionNode>();
            if (!argument_function || argument_function->getFunctionName() != "equals")
            {
                and_operands.push_back(argument);
                continue;
            }

            const auto & equals_arguments = argument_function->getArguments().getNodes();
            const auto & lhs = equals_arguments[0];
            const auto & rhs = equals_arguments[1];

            const auto has_and_with_different_constant = [&](const QueryTreeNodePtr & expression, const ConstantNode * constant)
            {
                if (auto it = node_to_constants.find(expression); it != node_to_constants.end())
                {
                    if (!it->second->isEqual(*constant))
                        return true;
                }
                else
                {
                    node_to_constants.emplace(expression, constant);
                    and_operands.push_back(argument);
                }

                return false;
            };

            bool collapse_to_false = false;

            if (const auto * lhs_literal = lhs->as<ConstantNode>())
                collapse_to_false = has_and_with_different_constant(rhs, lhs_literal);
            else if (const auto * rhs_literal = rhs->as<ConstantNode>())
                collapse_to_false = has_and_with_different_constant(lhs, rhs_literal);
            else
                and_operands.push_back(argument);

            if (collapse_to_false)
            {
                auto false_value = std::make_shared<ConstantValue>(0u, function_node.getResultType());
                auto false_node = std::make_shared<ConstantNode>(std::move(false_value));
                node = std::move(false_node);
                return;
            }
        }

        if (and_operands.size() == function_node.getArguments().getNodes().size())
            return;

        if (and_operands.size() == 1)
        {
            /// AND operator can have UInt8 or bool as its type.
            /// bool is used if a bool constant is at least one operand.
            /// Because we reduce the number of operands here by eliminating the same equality checks,
            /// the only situation we can end up here is we had AND check where all the equality checks are the same so we know the type is UInt8.
            /// Otherwise, we will have > 1 operands and we don't have to do anything.
            assert(!function_node.getResultType()->isNullable() && and_operands[0]->getResultType()->equals(*function_node.getResultType()));
            node = std::move(and_operands[0]);
            return;
        }

        auto and_function_resolver = FunctionFactory::instance().get("and", getContext());
        function_node.getArguments().getNodes() = std::move(and_operands);
        function_node.resolveAsFunction(and_function_resolver);
    }

    void tryReplaceOrEqualsChainWithIn(QueryTreeNodePtr & node)
    {
        auto & function_node = node->as<FunctionNode &>();
        assert(function_node.getFunctionName() == "or");

        QueryTreeNodes or_operands;

        QueryTreeNodePtrWithHashMap<QueryTreeNodes> node_to_equals_functions;
        QueryTreeNodePtrWithHashMap<QueryTreeNodeConstRawPtrWithHashSet> node_to_constants;

        for (const auto & argument : function_node.getArguments())
        {
            auto * argument_function = argument->as<FunctionNode>();
            if (!argument_function || argument_function->getFunctionName() != "equals")
            {
                or_operands.push_back(argument);
                continue;
            }

            /// collect all equality checks (x = value)

            const auto & equals_arguments = argument_function->getArguments().getNodes();
            const auto & lhs = equals_arguments[0];
            const auto & rhs = equals_arguments[1];

            const auto add_equals_function_if_not_present = [&](const auto & expression_node, const ConstantNode * constant)
            {
                auto & constant_set = node_to_constants[expression_node];
                if (!constant_set.contains(constant))
                {
                    constant_set.insert(constant);
                    node_to_equals_functions[expression_node].push_back(argument);
                }
            };

            if (const auto * lhs_literal = lhs->as<ConstantNode>();
                lhs_literal && !lhs_literal->getValue().isNull())
                add_equals_function_if_not_present(rhs, lhs_literal);
            else if (const auto * rhs_literal = rhs->as<ConstantNode>();
                     rhs_literal && !rhs_literal->getValue().isNull())
                add_equals_function_if_not_present(lhs, rhs_literal);
            else
                or_operands.push_back(argument);
        }

        auto in_function_resolver = FunctionFactory::instance().get("in", getContext());

        for (auto & [expression, equals_functions] : node_to_equals_functions)
        {
            const auto & settings = getSettings();
            if (equals_functions.size() < settings.optimize_min_equality_disjunction_chain_length && !expression.node->getResultType()->lowCardinality())
            {
                std::move(equals_functions.begin(), equals_functions.end(), std::back_inserter(or_operands));
                continue;
            }

            Tuple args;
            args.reserve(equals_functions.size());
            /// first we create tuple from RHS of equals functions
            for (const auto & equals : equals_functions)
            {
                const auto * equals_function = equals->as<FunctionNode>();
                assert(equals_function && equals_function->getFunctionName() == "equals");

                const auto & equals_arguments = equals_function->getArguments().getNodes();
                if (const auto * rhs_literal = equals_arguments[1]->as<ConstantNode>())
                {
                    args.push_back(rhs_literal->getValue());
                }
                else
                {
                    const auto * lhs_literal = equals_arguments[0]->as<ConstantNode>();
                    assert(lhs_literal);
                    args.push_back(lhs_literal->getValue());
                }
            }

            auto rhs_node = std::make_shared<ConstantNode>(std::move(args));

            auto in_function = std::make_shared<FunctionNode>("in");

            QueryTreeNodes in_arguments;
            in_arguments.reserve(2);
            in_arguments.push_back(expression.node);
            in_arguments.push_back(std::move(rhs_node));

            in_function->getArguments().getNodes() = std::move(in_arguments);
            in_function->resolveAsFunction(in_function_resolver);

            or_operands.push_back(std::move(in_function));
        }

        if (or_operands.size() == function_node.getArguments().getNodes().size())
            return;

        if (or_operands.size() == 1)
        {
            /// if the result type of operand is the same as the result type of OR
            /// we can replace OR with the operand
            if (or_operands[0]->getResultType()->equals(*function_node.getResultType()))
            {
                node = std::move(or_operands[0]);
                return;
            }

            /// otherwise add a stub 0 to make OR correct
            or_operands.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(0)));
        }

        auto or_function_resolver = FunctionFactory::instance().get("or", getContext());
        function_node.getArguments().getNodes() = std::move(or_operands);
        function_node.resolveAsFunction(or_function_resolver);
    }
};

void LogicalExpressionOptimizerPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    LogicalExpressionOptimizerVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
