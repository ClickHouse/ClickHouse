#include <Analyzer/Passes/LogicalExpressionOptimizerPass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/Utils.h>

#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using namespace std::literals;
static constexpr std::array boolean_functions{
    "equals"sv,   "notEquals"sv,   "less"sv,   "greaterOrEquals"sv, "greater"sv,      "lessOrEquals"sv,    "in"sv,     "notIn"sv,
    "globalIn"sv, "globalNotIn"sv, "nullIn"sv, "notNullIn"sv,       "globalNullIn"sv, "globalNullNotIn"sv, "isNull"sv, "isNotNull"sv,
    "like"sv,     "notLike"sv,     "ilike"sv,  "notILike"sv,        "empty"sv,        "notEmpty"sv,        "not"sv,    "and"sv,
    "or"sv};

static bool isBooleanFunction(const String & func_name)
{
    return std::any_of(
        boolean_functions.begin(), boolean_functions.end(), [&](const auto boolean_func) { return func_name == boolean_func; });
}

/// Visitor that optimizes logical expressions _only_ in JOIN ON section
class JoinOnLogicalExpressionOptimizerVisitor : public InDepthQueryTreeVisitorWithContext<JoinOnLogicalExpressionOptimizerVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<JoinOnLogicalExpressionOptimizerVisitor>;

    explicit JoinOnLogicalExpressionOptimizerVisitor(const JoinNode * join_node_, ContextPtr context)
        : Base(std::move(context))
        , join_node(join_node_)
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return;

        if (function_node->getFunctionName() == "or")
        {
            bool is_argument_type_changed = tryOptimizeIsNotDistinctOrIsNull(node, getContext());
            if (is_argument_type_changed)
                need_rerun_resolve = true;
            return;
        }
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!need_rerun_resolve)
            return;

        if (auto * function_node = node->as<FunctionNode>())
            rerunFunctionResolve(function_node, getContext());
    }

private:
    const JoinNode * join_node;
    bool need_rerun_resolve = false;

    /// Returns true if type of some operand is changed and parent function needs to be re-resolved
    bool tryOptimizeIsNotDistinctOrIsNull(QueryTreeNodePtr & node, const ContextPtr & context)
    {
        auto & function_node = node->as<FunctionNode &>();
        chassert(function_node.getFunctionName() == "or");


        QueryTreeNodes or_operands;
        or_operands.reserve(function_node.getArguments().getNodes().size());

        /// Indices of `equals` or `isNotDistinctFrom` functions in the vector above
        std::vector<size_t> equals_functions_indices;

        /** Map from `isNull` argument to indices of operands that contains that `isNull` functions
          * `a = b OR (a IS NULL AND b IS NULL) OR (a IS NULL AND c IS NULL)`
          * will be mapped to
          * {
          *     a => [(a IS NULL AND b IS NULL), (a IS NULL AND c IS NULL)]
          *     b => [(a IS NULL AND b IS NULL)]
          *     c => [(a IS NULL AND c IS NULL)]
          * }
          * Then for each a <=> b we can find all operands that contains both a IS NULL and b IS NULL
          */
        QueryTreeNodePtrWithHashMap<std::vector<size_t>> is_null_argument_to_indices;

        for (const auto & argument : function_node.getArguments())
        {
            or_operands.push_back(argument);

            auto * argument_function = argument->as<FunctionNode>();
            if (!argument_function)
                continue;

            const auto & func_name = argument_function->getFunctionName();
            if (func_name == "equals" || func_name == "isNotDistinctFrom")
            {
                const auto & argument_nodes = argument_function->getArguments().getNodes();
                if (argument_nodes.size() != 2)
                    continue;
                /// We can rewrite to a <=> b only if we are joining on a and b,
                /// because the function is not yet implemented for other cases.
                auto first_src = getExpressionSource(argument_nodes[0]);
                auto second_src = getExpressionSource(argument_nodes[1]);
                if (!first_src || !second_src)
                    continue;
                const auto & lhs_join = *join_node->getLeftTableExpression();
                const auto & rhs_join = *join_node->getRightTableExpression();
                bool arguments_from_both_sides = (first_src->isEqual(lhs_join) && second_src->isEqual(rhs_join)) ||
                                                 (first_src->isEqual(rhs_join) && second_src->isEqual(lhs_join));
                if (!arguments_from_both_sides)
                    continue;
                equals_functions_indices.push_back(or_operands.size() - 1);
            }
            else if (func_name == "and")
            {
                for (const auto & and_argument : argument_function->getArguments().getNodes())
                {
                    auto * and_argument_function = and_argument->as<FunctionNode>();
                    if (and_argument_function && and_argument_function->getFunctionName() == "isNull")
                    {
                        const auto & is_null_argument = and_argument_function->getArguments().getNodes()[0];
                        is_null_argument_to_indices[is_null_argument].push_back(or_operands.size() - 1);
                    }
                }
            }
        }

        /// OR operands that are changed to and needs to be re-resolved
        std::unordered_set<size_t> arguments_to_reresolve;

        for (size_t equals_function_idx : equals_functions_indices)
        {
            auto * equals_function = or_operands[equals_function_idx]->as<FunctionNode>();

            /// For a <=> b we are looking for expressions containing both `a IS NULL` and `b IS NULL` combined with AND
            const auto & argument_nodes = equals_function->getArguments().getNodes();
            const auto & lhs_is_null_parents = is_null_argument_to_indices[argument_nodes[0]];
            const auto & rhs_is_null_parents = is_null_argument_to_indices[argument_nodes[1]];
            std::unordered_set<size_t> operands_to_optimize;
            std::set_intersection(lhs_is_null_parents.begin(), lhs_is_null_parents.end(),
                                  rhs_is_null_parents.begin(), rhs_is_null_parents.end(),
                                  std::inserter(operands_to_optimize, operands_to_optimize.begin()));

            /// If we have `a = b OR (a IS NULL AND b IS NULL)` we can optimize it to `a <=> b`
            if (!operands_to_optimize.empty() && equals_function->getFunctionName() == "equals")
                arguments_to_reresolve.insert(equals_function_idx);

            for (size_t to_optimize_idx : operands_to_optimize)
            {
                /// We are looking for operand `a IS NULL AND b IS NULL AND ...`
                auto * operand_to_optimize = or_operands[to_optimize_idx]->as<FunctionNode>();

                /// Remove `a IS NULL` and `b IS NULL` arguments from AND
                QueryTreeNodes new_arguments;
                for (const auto & and_argument : operand_to_optimize->getArguments().getNodes())
                {
                    bool to_eliminate = false;

                    const auto * and_argument_function = and_argument->as<FunctionNode>();
                    if (and_argument_function && and_argument_function->getFunctionName() == "isNull")
                    {
                        const auto & is_null_argument = and_argument_function->getArguments().getNodes()[0];
                        to_eliminate = (is_null_argument->isEqual(*argument_nodes[0]) || is_null_argument->isEqual(*argument_nodes[1]));
                    }

                    if (to_eliminate)
                        arguments_to_reresolve.insert(to_optimize_idx);
                    else
                        new_arguments.emplace_back(and_argument);
                }
                /// If less than two arguments left, we will remove or replace the whole AND below
                operand_to_optimize->getArguments().getNodes() = std::move(new_arguments);
            }
        }

        if (arguments_to_reresolve.empty())
            /// Nothing have been changed
            return false;

        auto and_function_resolver = FunctionFactory::instance().get("and", context);
        auto strict_equals_function_resolver = FunctionFactory::instance().get("isNotDistinctFrom", context);

        bool need_reresolve = false;
        QueryTreeNodes new_or_operands;
        for (size_t i = 0; i < or_operands.size(); ++i)
        {
            if (arguments_to_reresolve.contains(i))
            {
                auto * function = or_operands[i]->as<FunctionNode>();
                if (function->getFunctionName() == "equals")
                {
                    /// We should replace `a = b` with `a <=> b` because we removed checks for IS NULL
                    need_reresolve |= function->getResultType()->isNullable();
                    function->resolveAsFunction(strict_equals_function_resolver);
                    new_or_operands.emplace_back(std::move(or_operands[i]));
                }
                else if (function->getFunctionName() == "and")
                {
                    const auto & and_arguments = function->getArguments().getNodes();
                    if (and_arguments.size() > 1)
                    {
                        function->resolveAsFunction(and_function_resolver);
                        new_or_operands.emplace_back(std::move(or_operands[i]));
                    }
                    else if (and_arguments.size() == 1)
                    {
                        /// Replace AND with a single argument with the argument itself
                        new_or_operands.emplace_back(and_arguments[0]);
                    }
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function name: '{}'", function->getFunctionName());
            }
            else
            {
                new_or_operands.emplace_back(std::move(or_operands[i]));
            }
        }

        if (new_or_operands.size() == 1)
        {
            node = std::move(new_or_operands[0]);
            return need_reresolve;
        }

        /// Rebuild OR function
        auto or_function_resolver = FunctionFactory::instance().get("or", context);
        function_node.getArguments().getNodes() = std::move(new_or_operands);
        function_node.resolveAsFunction(or_function_resolver);
        return need_reresolve;
    }
};

class LogicalExpressionOptimizerVisitor : public InDepthQueryTreeVisitorWithContext<LogicalExpressionOptimizerVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<LogicalExpressionOptimizerVisitor>;

    explicit LogicalExpressionOptimizerVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * join_node = node->as<JoinNode>())
        {
            /// Operator <=> is not supported outside of JOIN ON section
            if (join_node->hasJoinExpression())
            {
                JoinOnLogicalExpressionOptimizerVisitor join_on_visitor(join_node, getContext());
                join_on_visitor.visit(join_node->getJoinExpression());
            }
            return;
        }

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
            tryOptimizeAndEqualsNotEqualsChain(node);
            return;
        }

        if (function_node->getFunctionName() == "equals")
        {
            tryOptimizeOutRedundantEquals(node);
            return;
        }
    }

private:
    void tryOptimizeAndEqualsNotEqualsChain(QueryTreeNodePtr & node)
    {
        auto & function_node = node->as<FunctionNode &>();
        assert(function_node.getFunctionName() == "and");

        if (function_node.getResultType()->isNullable())
            return;

        QueryTreeNodes and_operands;

        QueryTreeNodePtrWithHashMap<const ConstantNode *> equals_node_to_constants;
        QueryTreeNodePtrWithHashMap<QueryTreeNodeConstRawPtrWithHashSet> not_equals_node_to_constants;
        QueryTreeNodePtrWithHashMap<QueryTreeNodes> node_to_not_equals_functions;

        for (const auto & argument : function_node.getArguments())
        {
            auto * argument_function = argument->as<FunctionNode>();
            const auto valid_functions = std::unordered_set<std::string>{"equals", "notEquals"};
            if (!argument_function || !valid_functions.contains(argument_function->getFunctionName()))
            {
                and_operands.push_back(argument);
                continue;
            }

            const auto function_name = argument_function->getFunctionName();
            const auto & function_arguments = argument_function->getArguments().getNodes();
            const auto & lhs = function_arguments[0];
            const auto & rhs = function_arguments[1];

            if (function_name == "equals")
            {
                const auto has_and_with_different_constant = [&](const QueryTreeNodePtr & expression, const ConstantNode * constant)
                {
                    if (auto it = equals_node_to_constants.find(expression); it != equals_node_to_constants.end())
                    {
                        if (!it->second->isEqual(*constant))
                            return true;
                    }
                    else
                    {
                        equals_node_to_constants.emplace(expression, constant);
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
            else if (function_name == "notEquals")
            {
                 /// collect all inequality checks (x <> value)

                const auto add_not_equals_function_if_not_present = [&](const auto & expression_node, const ConstantNode * constant)
                {
                    auto & constant_set = not_equals_node_to_constants[expression_node];
                    if (!constant_set.contains(constant))
                    {
                        constant_set.insert(constant);
                        node_to_not_equals_functions[expression_node].push_back(argument);
                    }
                };

                if (const auto * lhs_literal = lhs->as<ConstantNode>();
                    lhs_literal && !lhs_literal->getValue().isNull())
                    add_not_equals_function_if_not_present(rhs, lhs_literal);
                else if (const auto * rhs_literal = rhs->as<ConstantNode>();
                        rhs_literal && !rhs_literal->getValue().isNull())
                    add_not_equals_function_if_not_present(lhs, rhs_literal);
                else
                    and_operands.push_back(argument);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function name: '{}'", function_name);
        }

        auto not_in_function_resolver = FunctionFactory::instance().get("notIn", getContext());

        for (auto & [expression, not_equals_functions] : node_to_not_equals_functions)
        {
            const auto & settings = getSettings();
            if (not_equals_functions.size() < settings.optimize_min_inequality_conjunction_chain_length && !expression.node->getResultType()->lowCardinality())
            {
                std::move(not_equals_functions.begin(), not_equals_functions.end(), std::back_inserter(and_operands));
                continue;
            }

            Tuple args;
            args.reserve(not_equals_functions.size());
            /// first we create tuple from RHS of notEquals functions
            for (const auto & not_equals : not_equals_functions)
            {
                const auto * not_equals_function = not_equals->as<FunctionNode>();
                assert(not_equals_function && not_equals_function->getFunctionName() == "notEquals");

                const auto & not_equals_arguments = not_equals_function->getArguments().getNodes();
                if (const auto * rhs_literal = not_equals_arguments[1]->as<ConstantNode>())
                {
                    args.push_back(rhs_literal->getValue());
                }
                else
                {
                    const auto * lhs_literal = not_equals_arguments[0]->as<ConstantNode>();
                    assert(lhs_literal);
                    args.push_back(lhs_literal->getValue());
                }
            }

            auto rhs_node = std::make_shared<ConstantNode>(std::move(args));

            auto not_in_function = std::make_shared<FunctionNode>("notIn");

            QueryTreeNodes not_in_arguments;
            not_in_arguments.reserve(2);
            not_in_arguments.push_back(expression.node);
            not_in_arguments.push_back(std::move(rhs_node));

            not_in_function->getArguments().getNodes() = std::move(not_in_arguments);
            not_in_function->resolveAsFunction(not_in_function_resolver);

            and_operands.push_back(std::move(not_in_function));
        }

        if (and_operands.size() == function_node.getArguments().getNodes().size())
            return;

        if (and_operands.size() == 1)
        {
            /// AND operator can have UInt8 or bool as its type.
            /// bool is used if a bool constant is at least one operand.

            auto operand_type = and_operands[0]->getResultType();
            auto function_type = function_node.getResultType();
            chassert(!function_type->isNullable());
            if (!function_type->equals(*operand_type))
            {
                /// Result of equality operator can be low cardinality, while AND always returns UInt8.
                /// In that case we replace `(lc = 1) AND (lc = 1)` with `(lc = 1) AS UInt8`
                chassert(function_type->equals(*removeLowCardinality(operand_type)));
                node = createCastFunction(std::move(and_operands[0]), function_type, getContext());
            }
            else
            {
                node = std::move(and_operands[0]);
            }
            return;
        }

        auto and_function_resolver = FunctionFactory::instance().get("and", getContext());
        function_node.getArguments().getNodes() = std::move(and_operands);
        function_node.resolveAsFunction(and_function_resolver);
    }

    void tryReplaceOrEqualsChainWithIn(QueryTreeNodePtr & node)
    {
        auto & function_node = node->as<FunctionNode &>();
        chassert(function_node.getFunctionName() == "or");

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

            bool is_any_nullable = false;
            Tuple args;
            args.reserve(equals_functions.size());
            /// first we create tuple from RHS of equals functions
            for (const auto & equals : equals_functions)
            {
                is_any_nullable |= removeLowCardinality(equals->getResultType())->isNullable();

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
            /** For `k :: UInt8`, expression `k = 1 OR k = NULL` with result type Nullable(UInt8)
              * is replaced with `k IN (1, NULL)` with result type UInt8.
              * Convert it back to Nullable(UInt8).
              */
            if (is_any_nullable && !in_function->getResultType()->isNullable())
            {
                auto nullable_result_type = std::make_shared<DataTypeNullable>(in_function->getResultType());
                auto in_function_nullable = createCastFunction(std::move(in_function), std::move(nullable_result_type), getContext());
                or_operands.push_back(std::move(in_function_nullable));
            }
            else
            {
                or_operands.push_back(std::move(in_function));
            }
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
            or_operands.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(0), function_node.getResultType()));
        }

        auto or_function_resolver = FunctionFactory::instance().get("or", getContext());
        function_node.getArguments().getNodes() = std::move(or_operands);
        function_node.resolveAsFunction(or_function_resolver);
    }

    void tryOptimizeOutRedundantEquals(QueryTreeNodePtr & node)
    {
        auto & function_node = node->as<FunctionNode &>();
        assert(function_node.getFunctionName() == "equals");

        const auto function_arguments = function_node.getArguments().getNodes();
        if (function_arguments.size() != 2)
            return;

        const auto & lhs = function_arguments[0];
        const auto & rhs = function_arguments[1];

        UInt64 constant_value;
        bool is_lhs_const;
        if (const auto * lhs_constant = lhs->as<ConstantNode>())
        {
            if (!lhs_constant->getValue().tryGet<UInt64>(constant_value) || constant_value > 1
                || isNullableOrLowCardinalityNullable(lhs_constant->getResultType()))
                return;
            is_lhs_const = true;
        }
        else if (const auto * rhs_constant = rhs->as<ConstantNode>())
        {
            if (!rhs_constant->getValue().tryGet<UInt64>(constant_value) || constant_value > 1
                || isNullableOrLowCardinalityNullable(rhs_constant->getResultType()))
                return;
            is_lhs_const = false;
        }
        else
            return;

        const FunctionNode * child_function = is_lhs_const ? rhs->as<FunctionNode>() : lhs->as<FunctionNode>();
        if (!child_function || !isBooleanFunction(child_function->getFunctionName()))
            return;

        // if we have something like `function = 0`, we need to add a `NOT` when dropping the `= 0`
        if (constant_value == 0)
        {
            auto not_resolver = FunctionFactory::instance().get("not", getContext());
            const auto not_node = std::make_shared<FunctionNode>("not");
            auto & arguments = not_node->getArguments().getNodes();
            arguments.reserve(1);
            arguments.push_back(is_lhs_const ? rhs : lhs);
            not_node->resolveAsFunction(not_resolver->build(not_node->getArgumentColumns()));
            node = not_node;
        }
        else
            node = is_lhs_const ? rhs : lhs;
    }
};

void LogicalExpressionOptimizerPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    LogicalExpressionOptimizerVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
