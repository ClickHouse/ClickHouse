#include <Analyzer/Passes/LogicalExpressionOptimizerPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#include <iostream>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 optimize_min_equality_disjunction_chain_length;
    extern const SettingsUInt64 optimize_min_inequality_conjunction_chain_length;
    extern const SettingsBool optimize_extract_common_expressions;
    extern const SettingsBool optimize_and_compare_chain;
}

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


bool isBooleanFunction(const String & func_name)
{
    return std::any_of(
        boolean_functions.begin(), boolean_functions.end(), [&](const auto boolean_func) { return func_name == boolean_func; });
}

bool isNodeFunction(const QueryTreeNodePtr & node, const String & func_name)
{
    if (const auto * function_node = node->as<FunctionNode>())
        return function_node->getFunctionName() == func_name;
    return false;
}

QueryTreeNodePtr getFunctionArgument(const QueryTreeNodePtr & node, size_t idx)
{
    if (const auto * function_node = node->as<FunctionNode>())
    {
        const auto & args = function_node->getArguments().getNodes();
        if (idx < args.size())
            return args[idx];
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected '{}' to be a function with at least {} arguments", node->formatASTForErrorMessage(), idx + 1);
}

QueryTreeNodePtr findEqualsFunction(const QueryTreeNodes & nodes)
{
    for (const auto & node : nodes)
    {
        const auto * function_node = node->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "equals" &&
            function_node->getArguments().getNodes().size() == 2)
        {
            return node;
        }
    }
    return nullptr;
}

/// Checks if the node is combination of isNull and notEquals functions of two the same arguments:
/// [ (a <> b AND) ] (a IS NULL) AND (b IS NULL)
bool matchIsNullOfTwoArgs(const QueryTreeNodes & nodes, QueryTreeNodePtr & lhs, QueryTreeNodePtr & rhs)
{
    QueryTreeNodePtrWithHashSet all_arguments;
    QueryTreeNodePtrWithHashSet is_null_arguments;

    for (const auto & node : nodes)
    {
        const auto * func_node = node->as<FunctionNode>();
        if (!func_node)
            return false;

        const auto & arguments = func_node->getArguments().getNodes();
        if (func_node->getFunctionName() == "isNull" && arguments.size() == 1)
        {
            all_arguments.insert(QueryTreeNodePtrWithHash(arguments[0]));
            is_null_arguments.insert(QueryTreeNodePtrWithHash(arguments[0]));
        }

        else if (func_node->getFunctionName() == "notEquals" && arguments.size() == 2)
        {
            if (arguments[0]->isEqual(*arguments[1]))
                return false;
            all_arguments.insert(QueryTreeNodePtrWithHash(arguments[0]));
            all_arguments.insert(QueryTreeNodePtrWithHash(arguments[1]));
        }
        else
            return false;

        if (all_arguments.size() > 2)
            return false;
    }

    if (all_arguments.size() != 2 || is_null_arguments.size() != 2)
        return false;

    lhs = all_arguments.begin()->node;
    rhs = std::next(all_arguments.begin())->node;
    return true;
}

bool isBooleanConstant(const QueryTreeNodePtr & node, bool expected_value)
{
    const auto * constant_node = node->as<ConstantNode>();
    if (!constant_node || !constant_node->getResultType()->equals(DataTypeUInt8()))
        return false;

    UInt64 constant_value;
    return (constant_node->getValue().tryGet<UInt64>(constant_value) && constant_value == expected_value);
}

/// Returns true if expression consists of only conjunctions of functions with the specified name or true constants
bool isOnlyConjunctionOfFunctions(
    const QueryTreeNodePtr & node,
    const String & func_name,
    const QueryTreeNodePtrWithHashSet & allowed_arguments)
{
    if (isBooleanConstant(node, true))
        return true;

    const auto * node_function = node->as<FunctionNode>();
    if (!node_function)
        return false;

    if (node_function->getFunctionName() == func_name
        && allowed_arguments.contains(node_function->getArgumentsNode()))
        return true;

    if (node_function->getFunctionName() == "and")
    {
        for (const auto & and_argument : node_function->getArguments().getNodes())
        {
            if (!isOnlyConjunctionOfFunctions(and_argument, func_name, allowed_arguments))
                return false;
        }
        return true;
    }
    return false;
}

/// We can rewrite to a <=> b only if we are joining on a and b,
/// because the function is not yet implemented for other cases.
bool isTwoArgumentsFromDifferentSides(const FunctionNode & node_function, const JoinNode & join_node)
{
    const auto & argument_nodes = node_function.getArguments().getNodes();
    if (argument_nodes.size() != 2)
        return false;

    auto first_src = getExpressionSource(argument_nodes[0]);
    auto second_src = getExpressionSource(argument_nodes[1]);
    if (!first_src || !second_src)
        return false;

    const auto & lhs_join = *join_node.getLeftTableExpression();
    const auto & rhs_join = *join_node.getRightTableExpression();
    return (first_src->isEqual(lhs_join) && second_src->isEqual(rhs_join)) ||
           (first_src->isEqual(rhs_join) && second_src->isEqual(lhs_join));
}

void insertIfNotPresentInSet(QueryTreeNodePtrWithHashSet& set, QueryTreeNodes &nodes, QueryTreeNodePtr node)
{
    const auto [_, inserted] = set.emplace(node);
    if (inserted)
        nodes.push_back(std::move(node));
}

// Returns the flattened AND/OR node if the passed-in node can be flattened. Doesn't modify the passed-in node.
std::shared_ptr<FunctionNode> getFlattenedLogicalExpression(const FunctionNode & node, const ContextPtr & context)
{
    const auto & function_name = node.getFunctionName();
    if (function_name != "or" && function_name != "and")
        return nullptr;

    const auto & arguments = node.getArguments().getNodes();
    QueryTreeNodes new_arguments;

    bool flattened_anything = false;

    for (const auto & argument : arguments)
    {
        auto * maybe_function = argument->as<FunctionNode>();
        // If the nested function is not the same, just use it as is
        if (!maybe_function || maybe_function->getFunctionName() != function_name)
        {
            new_arguments.push_back(argument);
            continue;
        }

        flattened_anything = true;

        // If the nested function is the same, just lift the its or its flattened form's arguments
        auto maybe_flattened = getFlattenedLogicalExpression(*maybe_function, context);
        if (maybe_flattened)
        {
            auto & flattened_arguments = maybe_flattened->getArguments().getNodes();
            std::move(flattened_arguments.begin(), flattened_arguments.end(), std::back_inserter(new_arguments));
        }
        else
        {
            const auto & nested_arguments = maybe_function->getArguments().getNodes();
            std::copy(nested_arguments.begin(), nested_arguments.end(), std::back_inserter(new_arguments));
        }
    }

    // Nothing has changed, let's no create a flattened node
    if (!flattened_anything && new_arguments.size() == arguments.size())
        return {};

    auto flattened = std::make_shared<FunctionNode>(function_name);

    flattened->getArguments().getNodes() = std::move(new_arguments);

    resolveOrdinaryFunctionNodeByName(*flattened, function_name, context);

    return flattened;
}

struct CommonExpressionExtractionResult
{
    // new_node: if the new node is not empty, then it contains the new node, otherwise nullptr
    // common_expressions: the extracted common expressions. The new expressions can be created
    // as the conjunction of new_node and the nodes in common_expressions. It is guaranteed that
    // the common expressions are deduplicated.
    // Examples:
    //   Input: (A & B & C) | (A & D & E)
    //   Result: new_node = (B & C) | (D & E), common_expressions = {A}
    //
    //   Input: (A & B) | (A & B)
    //   Result: new_node = nullptr, common_expressions = {A, B}
    //
    //   This is a special case: A & B & C is a subset of A & B, thus the conjunction of extracted
    //   expressions is equivalent with the passed-in expression, we have to discard C. With C the
    //   new expression would be more restrictive.
    //   Input: (A & B) | (A & B & C)
    //   Result: new_node = nullptr, common_expressions = {A, B}
    QueryTreeNodePtr new_node;
    QueryTreeNodes common_expressions;
};

// Optimize disjuctions by extracting common expressions in disjuncts.
// Example: A or B or (B and C)
// Result: A or B
std::optional<CommonExpressionExtractionResult> tryExtractCommonExpressionsInDisjunction(const QueryTreeNodes & disjuncts, const ContextPtr & context)
{
    std::vector<QueryTreeNodePtrWithHashSet> disjunct_sets;
    disjunct_sets.reserve(disjuncts.size());
    for (const auto & disjunct : disjuncts)
    {
        QueryTreeNodePtrWithHashSet disjunct_set;

        auto * disjunct_function = disjunct->as<FunctionNode>();
        if (disjunct_function != nullptr && disjunct_function->getFunctionName() == "and")
        {
            auto & arguments = disjunct_function->getArguments();
            std::copy(arguments.begin(), arguments.end(), std::inserter(disjunct_set, disjunct_set.end()));
        }
        else
        {
            disjunct_set.insert(disjunct);
        }
        disjunct_sets.emplace_back(std::move(disjunct_set));
    }

    std::vector<bool> should_keep(disjuncts.size(), true);
    size_t removed = 0;

    for (size_t i = 0; i < disjuncts.size(); ++i)
    {
        if (!should_keep[i])
            continue;

        const auto & current_set = disjunct_sets[i];
        for (size_t j = 0; j < disjuncts.size(); ++j)
        {
            if (i == j || !should_keep[j])
                continue;

            bool is_subset = true;
            for (auto const & elem : current_set)
            {
                if (!disjunct_sets[j].contains(elem))
                {
                    is_subset = false;
                    break;
                }
            }

            if (is_subset)
            {
                should_keep[j] = false;
                ++removed;
            }
        }
    }

    if (removed == 0)
        return {};

    if (removed == disjuncts.size() - 1)
    {
        for (size_t i = 0; i < disjuncts.size(); ++i)
        {
            if (should_keep[i])
                return CommonExpressionExtractionResult{ .new_node = nullptr, .common_expressions = { disjuncts[i] } };
        }
    }

    QueryTreeNodes new_disjuncts;
    new_disjuncts.reserve(disjuncts.size() - removed);
    for (size_t i = 0; i < disjuncts.size(); ++i)
    {
        if (should_keep[i])
            new_disjuncts.emplace_back(disjuncts[i]);
    }

    auto new_or_node = std::make_shared<FunctionNode>("or");
    new_or_node->markAsOperator();
    new_or_node->getArguments().getNodes() = std::move(new_disjuncts);

    resolveOrdinaryFunctionNodeByName(*new_or_node, "or", context);
    return CommonExpressionExtractionResult{ .new_node = new_or_node, .common_expressions = {} };
}

std::optional<CommonExpressionExtractionResult> tryExtractCommonExpressions(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    auto * or_node = node->as<FunctionNode>();
    if (!or_node || or_node->getFunctionName() != "or")
        return {}; // the optimization can only be done on or nodes

    auto flattened_or_node = getFlattenedLogicalExpression(*or_node, context);
    if (flattened_or_node)
        or_node = flattened_or_node.get();

    auto & or_argument_nodes = or_node->getArguments().getNodes();

    chassert(or_argument_nodes.size() > 1);

    bool first_argument = true;
    QueryTreeNodePtrWithHashSet common_exprs_set;
    QueryTreeNodes common_exprs;
    QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> flattened_ands;

    for (auto & maybe_and_node : or_argument_nodes)
    {
        auto * and_node = maybe_and_node->as<FunctionNode>();
        if (!and_node || and_node->getFunctionName() != "and")
        {
            // There are no common expressions, but we can try to optimize disjuncts
            return tryExtractCommonExpressionsInDisjunction(or_argument_nodes, context);
        }

        auto flattened_and_node = getFlattenedLogicalExpression(*and_node, context);
        if (flattened_and_node)
        {
            flattened_ands.emplace(maybe_and_node, flattened_and_node);
            and_node = flattened_and_node.get();
        }

        if (first_argument)
        {
            auto & current_arguments = and_node->getArguments().getNodes();
            common_exprs.reserve(current_arguments.size());

            for (auto & and_argument : current_arguments)
                insertIfNotPresentInSet(common_exprs_set, common_exprs, and_argument);

            first_argument = false;
        }
        else
        {
            QueryTreeNodePtrWithHashSet new_common_exprs_set;
            QueryTreeNodes new_common_exprs;

            for (auto & and_argument : and_node->getArguments())
            {
                if (common_exprs_set.contains(and_argument))
                    insertIfNotPresentInSet(new_common_exprs_set, new_common_exprs, and_argument);
            }

            common_exprs_set = std::move(new_common_exprs_set);
            common_exprs = std::move(new_common_exprs);

            if (common_exprs.empty())
            {
                // There are no common expressions, but we can try to optimize disjuncts
                return tryExtractCommonExpressionsInDisjunction(or_argument_nodes, context);
            }
        }
    }

    chassert(!common_exprs.empty());

    QueryTreeNodePtrWithHashSet new_or_arguments_set;
    QueryTreeNodes new_or_arguments;
    bool has_completely_extracted_and_expression = false;

    for (auto & or_argument : or_argument_nodes)
    {
        if (auto it = flattened_ands.find(or_argument); it != flattened_ands.end())
            or_argument = it->second;

        // Avoid changing the original tree, it might be used later
        const auto & and_node = or_argument->as<FunctionNode &>();
        const auto & and_arguments = and_node.getArguments().getNodes();

        QueryTreeNodes filtered_and_arguments;
        filtered_and_arguments.reserve(and_arguments.size());
        std::copy_if(
            and_arguments.begin(),
            and_arguments.end(),
            std::back_inserter(filtered_and_arguments),
            [&common_exprs_set](const QueryTreeNodePtr & ptr) { return !common_exprs_set.contains(ptr); });

        if (filtered_and_arguments.empty())
        {
            has_completely_extracted_and_expression = true;
            // As we will discard new_or_arguments, no need for further processing
            break;
        }
        else if (filtered_and_arguments.size() == 1)
        {
            insertIfNotPresentInSet(new_or_arguments_set, new_or_arguments, std::move(filtered_and_arguments.front()));
        }
        else
        {
            auto new_and_node = std::make_shared<FunctionNode>("and");
            new_and_node->markAsOperator();
            new_and_node->getArguments().getNodes() = std::move(filtered_and_arguments);
            resolveOrdinaryFunctionNodeByName(*new_and_node, "and", context);

            insertIfNotPresentInSet(new_or_arguments_set, new_or_arguments, std::move(new_and_node));
        }
    }

    // If all the arguments of the OR expression is eliminated or one argument is completely eliminated, there is no need for new node.
    if (new_or_arguments.empty() || has_completely_extracted_and_expression)
        return CommonExpressionExtractionResult{nullptr, std::move(common_exprs)};

    // There are at least two arguments in the passed-in OR expression, thus we either completely eliminated at least one arguments, or there should be at least 2 remaining arguments.
    // The complete elimination is handled above, so at this point we can be sure there are at least 2 arguments.
    chassert(new_or_arguments.size() >= 2);

    if (auto optimized_disjunction =  tryExtractCommonExpressionsInDisjunction(new_or_arguments, context))
    {
        auto new_disjunction_node = optimized_disjunction->new_node ? optimized_disjunction->new_node : optimized_disjunction->common_expressions[0];
        return CommonExpressionExtractionResult{ .new_node = std::move(new_disjunction_node), .common_expressions = std::move(common_exprs) };
    }

    auto new_or_node = std::make_shared<FunctionNode>("or");
    new_or_node->markAsOperator();
    new_or_node->getArguments().getNodes() = std::move(new_or_arguments);

    resolveOrdinaryFunctionNodeByName(*new_or_node, "or", context);

    return CommonExpressionExtractionResult{new_or_node, common_exprs};
}

void tryOptimizeCommonExpressionsInOr(QueryTreeNodePtr & node, const ContextPtr & context)
{
    [[maybe_unused]] auto * root_node = node->as<FunctionNode>();
    chassert(root_node && root_node->getFunctionName() == "or");

    QueryTreeNodePtr new_root_node{};

    if (auto maybe_result = tryExtractCommonExpressions(node, context); maybe_result.has_value())
    {
        auto & result = *maybe_result;
        QueryTreeNodes new_root_arguments = std::move(result.common_expressions);
        if (result.new_node != nullptr)
            new_root_arguments.push_back(std::move(result.new_node));

        if (new_root_arguments.size() == 1)
        {
            new_root_node = std::move(new_root_arguments.front());
        }
        else
        {
            // The OR expression must be replaced by and AND expression that will contain the common expressions
            // and the new_node, if it is not nullptr.
            auto new_function_node = std::make_shared<FunctionNode>("and");
            new_function_node->markAsOperator();
            new_function_node->getArguments().getNodes() = std::move(new_root_arguments);
            auto and_function_resolver = FunctionFactory::instance().get("and", context);
            new_function_node->resolveAsFunction(and_function_resolver);
            new_root_node = std::move(new_function_node);
        }

        if (!new_root_node->getResultType()->equals(*node->getResultType()))
            new_root_node = buildCastFunction(new_root_node, node->getResultType(), context);
        node = std::move(new_root_node);
    }
}

void tryOptimizeCommonExpressionsInAnd(QueryTreeNodePtr & node, const ContextPtr & context)
{
    auto * root_node = node->as<FunctionNode>();
    chassert(root_node && root_node->getFunctionName() == "and");

    QueryTreeNodePtrWithHashSet new_top_level_arguments_set;
    QueryTreeNodes new_top_level_arguments;

    auto insert_possible_new_top_level_arg = [&new_top_level_arguments_set, &new_top_level_arguments](QueryTreeNodePtr node_to_insert)
    {
        insertIfNotPresentInSet(new_top_level_arguments_set, new_top_level_arguments, std::move(node_to_insert));
    };
    auto extracted_something = false;

    for (const auto & argument : root_node->getArguments())
    {
        if (auto maybe_result = tryExtractCommonExpressions(argument, context))
        {
            extracted_something = true;
            auto & result = *maybe_result;
            if (result.new_node != nullptr)
                insert_possible_new_top_level_arg(std::move(result.new_node));
            for (auto& common_expr: result.common_expressions)
                insert_possible_new_top_level_arg(std::move(common_expr));
        }
        else
        {
            insert_possible_new_top_level_arg(argument);
        }
    }

    if (!extracted_something)
        return;

    auto and_function_node = std::make_shared<FunctionNode>("and");
    and_function_node->markAsOperator();
    and_function_node->getArguments().getNodes() = std::move(new_top_level_arguments);
    auto and_function_resolver = FunctionFactory::instance().get("and", context);
    and_function_node->resolveAsFunction(and_function_resolver);
    QueryTreeNodePtr new_root_node = and_function_node;

    if (!new_root_node->getResultType()->equals(*node->getResultType()))
        new_root_node = buildCastFunction(new_root_node, node->getResultType(), context);
    node = std::move(new_root_node);
}

void tryOptimizeCommonExpressions(QueryTreeNodePtr & node, FunctionNode& function_node, const ContextPtr & context)
{
    chassert(node.get() == &function_node);
    if (function_node.getFunctionName() == "or")
        tryOptimizeCommonExpressionsInOr(node, context);
    else if (function_node.getFunctionName() == "and")
        tryOptimizeCommonExpressionsInAnd(node, context);
}


/// Visitor that optimizes logical expressions _only_ in JOIN ON section
class JoinOnLogicalExpressionOptimizerVisitor : public InDepthQueryTreeVisitorWithContext<JoinOnLogicalExpressionOptimizerVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<JoinOnLogicalExpressionOptimizerVisitor>;

    explicit JoinOnLogicalExpressionOptimizerVisitor(const JoinNode & join_node_, ContextPtr context)
        : Base(std::move(context))
        , join_node(join_node_)
    {}

    bool needChildVisit(const QueryTreeNodePtr & parent, const QueryTreeNodePtr &)
    {
        /** Optimization can change the value of some expression from NULL to FALSE.
          * For example:
          * when `a` is `NULL`, the expression `a = b AND a IS NOT NULL` returns `NULL`
          * and it will be optimized to `a = b`, which returns `FALSE`.
          * This is valid for JOIN ON condition and for the functions `AND`/`OR` inside it.
          * (When we replace `AND`/`OR` operands from `NULL` to `FALSE`, the result value can also change only from `NULL` to `FALSE`)
          * However, in the general case, the result can be wrong.
          * For example, for NOT: `NOT NULL` is `NULL`, but `NOT FALSE` is `TRUE`.
          * Therefore, optimize only top-level expression or expressions inside `AND`/`OR`.
          */
        if (const auto * function_node = parent->as<FunctionNode>())
        {
            const auto & func_name = function_node->getFunctionName();
            return func_name == "or" || func_name == "and";
        }
        return parent->getNodeType() == QueryTreeNodeType::LIST;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        /** Alias to expression in JOIN ON section may be used in SELECT.
          * Optimization that is safe for JOIN ON may not be safe for SELECT, for example:
          * Values `NULL` and `false` are not equivalent in SELECT, so we cannot change type from Nullable(UInt8) to UInt8 there, while it's valid for `JOIN ON`.
          * Also, operator <=> can be used in JOIN ON, but not in SELECT, so we need to keep original expression `a = b OR isNull(a) AND isNull(b) there.
          *
          * FIXME: May be removed after https://github.com/ClickHouse/ClickHouse/pull/66143
          */
        if (node.use_count() > 1)
            node = node->clone();

        auto * function_node = node->as<FunctionNode>();

        QueryTreeNodePtr new_node = nullptr;
        if (function_node && function_node->getFunctionName() == "or")
            new_node = tryOptimizeJoinOnNulls(function_node->getArguments().getNodes(), getContext());
        else
            new_node = tryOptimizeJoinOnNulls({node}, getContext());

        if (new_node)
        {
            need_rerun_resolve |= !new_node->getResultType()->equals(*node->getResultType());
            node = new_node;
        }
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return;

        if (need_rerun_resolve)
            rerunFunctionResolve(function_node, getContext());

        // The optimization only makes sense on the top level
        if (node != join_node.getJoinExpression() || !getSettings()[Setting::optimize_extract_common_expressions])
            return;

        tryOptimizeCommonExpressions(node, *function_node, getContext());
    }

private:
    const JoinNode & join_node;
    bool need_rerun_resolve = false;

    /// Returns optimized node or nullptr if nothing have been changed
    QueryTreeNodePtr tryOptimizeJoinOnNulls(const QueryTreeNodes & nodes, const ContextPtr & context)
    {
        QueryTreeNodes or_operands;
        or_operands.reserve(nodes.size());

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
          * Then for each equality a = b we can check if we have operand (a IS NULL AND b IS NULL)
          */
        QueryTreeNodePtrWithHashMap<std::vector<size_t>> is_null_argument_to_indices;

        bool is_anything_changed = false;

        for (const auto & node : nodes)
        {
            if (isBooleanConstant(node, false))
            {
                /// Remove false constants from OR
                is_anything_changed = true;
                continue;
            }

            or_operands.push_back(node);
            auto * argument_function = node->as<FunctionNode>();
            if (!argument_function)
                continue;

            const auto & func_name = argument_function->getFunctionName();
            if (func_name == "equals" || func_name == "isNotDistinctFrom")
            {
                if (isTwoArgumentsFromDifferentSides(*argument_function, join_node))
                    equals_functions_indices.push_back(or_operands.size() - 1);
            }
            else if (func_name == "and")
            {
                const auto & and_arguments = argument_function->getArguments().getNodes();

                QueryTreeNodePtr is_null_lhs_arg;
                QueryTreeNodePtr is_null_rhs_arg;
                if (matchIsNullOfTwoArgs(and_arguments, is_null_lhs_arg, is_null_rhs_arg))
                {
                    is_null_argument_to_indices[is_null_lhs_arg].push_back(or_operands.size() - 1);
                    is_null_argument_to_indices[is_null_rhs_arg].push_back(or_operands.size() - 1);
                    continue;
                }

                /// Expression `a = b AND (a IS NOT NULL) AND true AND (b IS NOT NULL)` we can be replaced with `a = b`
                /// Even though this expression are not equivalent (first is NULL on NULLs, while second is FALSE),
                /// it is still correct since for JOIN ON condition NULL is treated as FALSE
                if (const auto & equals_function = findEqualsFunction(and_arguments))
                {
                    const auto & equals_arguments = equals_function->as<FunctionNode>()->getArguments().getNodes();
                    /// Expected isNotNull arguments
                    QueryTreeNodePtrWithHashSet allowed_arguments;
                    allowed_arguments.insert(QueryTreeNodePtrWithHash(std::make_shared<ListNode>(QueryTreeNodes{equals_arguments[0]})));
                    allowed_arguments.insert(QueryTreeNodePtrWithHash(std::make_shared<ListNode>(QueryTreeNodes{equals_arguments[1]})));

                    bool can_be_optimized = true;
                    for (const auto & and_argument : and_arguments)
                    {
                        if (and_argument.get() == equals_function.get())
                            continue;

                        if (isOnlyConjunctionOfFunctions(and_argument, "isNotNull", allowed_arguments))
                            continue;

                        can_be_optimized = false;
                        break;
                    }

                    if (can_be_optimized)
                    {
                        is_anything_changed = true;
                        or_operands.pop_back();
                        or_operands.push_back(equals_function);
                        if (isTwoArgumentsFromDifferentSides(equals_function->as<FunctionNode &>(), join_node))
                            equals_functions_indices.push_back(or_operands.size() - 1);
                    }
                }
            }
        }

        /// OR operands that are changed to and needs to be re-resolved
        std::unordered_set<size_t> arguments_to_reresolve;

        for (size_t equals_function_idx : equals_functions_indices)
        {
            const auto * equals_function = or_operands[equals_function_idx]->as<FunctionNode>();

            /// For a = b we are looking for all expressions `a IS NULL AND b IS NULL`
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
                /// Remove `a IS NULL AND b IS NULL`
                or_operands[to_optimize_idx] = nullptr;
                is_anything_changed = true;
            }
        }

        if (arguments_to_reresolve.empty() && !is_anything_changed)
            /// Nothing have been changed
            return nullptr;

        auto and_function_resolver = FunctionFactory::instance().get("and", context);
        auto strict_equals_function_resolver = FunctionFactory::instance().get("isNotDistinctFrom", context);

        QueryTreeNodes new_or_operands;
        for (size_t i = 0; i < or_operands.size(); ++i)
        {
            if (arguments_to_reresolve.contains(i))
            {
                const auto * function = or_operands[i]->as<FunctionNode>();
                if (function->getFunctionName() == "equals")
                {
                    /// We should replace `a = b` with `a <=> b` because we removed checks for IS NULL
                    auto new_function = or_operands[i]->clone();
                    new_function->as<FunctionNode>()->resolveAsFunction(strict_equals_function_resolver);
                    new_or_operands.emplace_back(std::move(new_function));
                }
                else if (function->getFunctionName() == "and")
                {
                    const auto & and_arguments = function->getArguments().getNodes();
                    if (and_arguments.size() > 1)
                    {
                        auto new_function = or_operands[i]->clone();
                        new_function->as<FunctionNode>()->resolveAsFunction(and_function_resolver);
                        new_or_operands.emplace_back(std::move(new_function));
                    }
                    else if (and_arguments.size() == 1)
                    {
                        /// Replace AND with a single argument by the argument itself
                        new_or_operands.emplace_back(and_arguments[0]);
                    }
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function '{}'", function->getFunctionName());
            }
            else if (or_operands[i])
            {
                new_or_operands.emplace_back(std::move(or_operands[i]));
            }
        }

        if (new_or_operands.empty())
            return nullptr;

        if (new_or_operands.size() == 1)
            return new_or_operands[0];

        /// Rebuild OR function
        auto function_node = std::make_shared<FunctionNode>("or");
        function_node->markAsOperator();
        function_node->getArguments().getNodes() = std::move(new_or_operands);
        resolveOrdinaryFunctionNodeByName(*function_node, "or", context);
        return function_node;
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
                JoinOnLogicalExpressionOptimizerVisitor join_on_visitor(*join_node, getContext());
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
            if (getSettings()[Setting::optimize_and_compare_chain])
                tryOptimizeAndCompareChain(node);
            return;
        }

        if (function_node->getFunctionName() == "equals")
        {
            tryOptimizeOutRedundantEquals(node);
            return;
        }
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_extract_common_expressions])
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        const auto try_optimize_if_function = [this](QueryTreeNodePtr & maybe_node)
        {
            if (!maybe_node)
                return;
            auto * function_node = maybe_node->as<FunctionNode>();
            if (!function_node)
                return;
            tryOptimizeCommonExpressions(maybe_node, *function_node, getContext());
        };

        // TODO: This optimization can also be applied to HAVING and QUALIFY clauses,
        // but it can be allowed if and only if every logical expression is optimized.
        // Example:
        // SELECT a FROM t GROUP BY <logical_expression> as a HAVING a
        // All the references of `a` should be optimized to produce a valid query.
        try_optimize_if_function(query_node->getWhere());
        try_optimize_if_function(query_node->getPrewhere());
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
                    auto false_node = std::make_shared<ConstantNode>(0u, function_node.getResultType());
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
            if (not_equals_functions.size() < settings[Setting::optimize_min_inequality_conjunction_chain_length]
                && !expression.node->getResultType()->lowCardinality())
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
            not_in_function->markAsOperator();

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

    void tryOptimizeAndCompareChain(QueryTreeNodePtr & node)
    {
        if (node->getNodeType() != QueryTreeNodeType::FUNCTION)
            return;

        auto & function_node = node->as<FunctionNode &>();
        if (function_node.getFunctionName() != "and" || function_node.getResultType()->isNullable())
            return;

        enum CompareType
        {
            less = 0,
            greater,
            lessOrEquals,
            greaterOrEquals,
            equals
        };

        /// Step 1: identify constants, and store comparing pairs in hash
        QueryTreeNodePtrWithHashSet greater_constants;
        QueryTreeNodePtrWithHashSet less_constants;
        /// Record a > b, a >= b, a == b pairs or a < b, a <= b, a == b pairs
        using QueryTreeNodeWithEquals = std::vector<std::pair<QueryTreeNodePtr, CompareType>>;
        using ComparePairs = QueryTreeNodePtrWithHashMap<QueryTreeNodeWithEquals>;
        ComparePairs greater_pairs;
        ComparePairs less_pairs;

        auto flattened_and_node = getFlattenedLogicalExpression(function_node, getContext());
        const auto & arguments = flattened_and_node ? flattened_and_node->getArguments().getNodes()
                                                    : function_node.getArguments().getNodes();

        for (const auto & argument : arguments)
        {
            auto * argument_function = argument->as<FunctionNode>();
            const auto valid_functions = std::unordered_set<std::string>{
                "less", "greater", "lessOrEquals", "greaterOrEquals", "equals"};
            if (!argument_function || !valid_functions.contains(argument_function->getFunctionName()))
                continue;

            const auto function_name = argument_function->getFunctionName();
            const auto & function_arguments = argument_function->getArguments().getNodes();
            const auto & lhs = function_arguments[0];
            const auto & rhs = function_arguments[1];

            if (function_name == "less")
            {
                if (rhs->as<ConstantNode>())
                    greater_constants.insert(rhs);
                greater_pairs[rhs].push_back({lhs, CompareType::less});
                if (lhs->as<ConstantNode>())
                    less_constants.insert(lhs);
                less_pairs[lhs].push_back({rhs, CompareType::greater});
            }
            else if (function_name == "greater")
            {
                if (lhs->as<ConstantNode>())
                    greater_constants.insert(lhs);
                greater_pairs[lhs].push_back({rhs, CompareType::less});
                if (rhs->as<ConstantNode>())
                    less_constants.insert(rhs);
                less_pairs[rhs].push_back({lhs, CompareType::greater});
            }
            else if (function_name == "lessOrEquals")
            {
                if (rhs->as<ConstantNode>())
                    greater_constants.insert(rhs);
                greater_pairs[rhs].push_back({lhs, CompareType::lessOrEquals});
                if (lhs->as<ConstantNode>())
                    less_constants.insert(lhs);
                less_pairs[lhs].push_back({rhs, CompareType::greaterOrEquals});
            }
            else if (function_name == "greaterOrEquals")
            {
                if (lhs->as<ConstantNode>())
                    greater_constants.insert(lhs);
                greater_pairs[lhs].push_back({rhs, CompareType::lessOrEquals});
                if (rhs->as<ConstantNode>())
                    less_constants.insert(rhs);
                less_pairs[rhs].push_back({lhs, CompareType::greaterOrEquals});
            }
            else if (function_name == "equals")
            {
                if (rhs->as<ConstantNode>())
                {
                    greater_constants.insert(rhs);
                    greater_pairs[rhs].push_back({lhs, CompareType::equals});
                    less_constants.insert(rhs);
                    less_pairs[rhs].push_back({lhs, CompareType::equals});
                }
                else if (lhs->as<ConstantNode>())
                {
                    greater_constants.insert(lhs);
                    greater_pairs[lhs].push_back({rhs, CompareType::equals});
                    less_constants.insert(lhs);
                    less_pairs[lhs].push_back({rhs, CompareType::equals});
                }
                else
                {
                    /// Bidirection, needs to record visited
                    greater_pairs[lhs].push_back({rhs, CompareType::equals});
                    greater_pairs[rhs].push_back({lhs, CompareType::equals});
                    less_pairs[lhs].push_back({rhs, CompareType::equals});
                    less_pairs[rhs].push_back({lhs, CompareType::equals});
                }
            }
        }

        /// To avoid endless loop in equal condition and during the DFS, for example, a>b AND b>a AND a<5,
        /// also avoid duplicate such as a>3 AND b>a AND c>b AND c>a.
        QueryTreeNodePtrWithHashSet visited;
        /// To avoid duplicates of equals when starting from both sides, i.e. large and small constant.
        QueryTreeNodePtrWithHashMap<std::unordered_set<const ConstantNode *>> equal_funcs;

        /// Step 2: populate from constants, to generate new comparing pair with constant in one side
        std::function<void(const ComparePairs &, QueryTreeNodePtr, const ConstantNode *, CompareType)> findPairs
            = [&](const ComparePairs & pairs, QueryTreeNodePtr current, const ConstantNode * constant, CompareType type)
        {
            if (auto it = pairs.find(current); it != pairs.end())
            {
                for (const auto & left : it->second)
                {
                    if (visited.contains(left.first))
                        continue;
                    visited.insert(left.first);

                    CompareType compare_type = std::min(type, left.second);

                    /// Non-sense to have both sides as constant, and no repeat of equal function
                    if (constant && !left.first->as<ConstantNode>()
                        && (compare_type != CompareType::equals || equal_funcs[left.first].insert(constant).second))
                    {
                        String compare_function_name;
                        if (compare_type == CompareType::less)
                            compare_function_name = "less";
                        else if (compare_type == CompareType::greater)
                            compare_function_name = "greater";
                        else if (compare_type == CompareType::lessOrEquals)
                            compare_function_name = "lessOrEquals";
                        else if (compare_type == CompareType::greaterOrEquals)
                            compare_function_name = "greaterOrEquals";
                        else if (compare_type == CompareType::equals)
                            compare_function_name = "equals";

                        const auto and_node = std::make_shared<FunctionNode>(compare_function_name);
                        and_node->markAsOperator();
                        and_node->getArguments().getNodes().push_back(left.first->clone());
                        and_node->getArguments().getNodes().push_back(constant->clone());
                        and_node->resolveAsFunction(
                            FunctionFactory::instance().get(compare_function_name, getContext()));
                        function_node.getArguments().getNodes().push_back(and_node);
                    }

                    findPairs(pairs, left.first, constant ? constant : current->as<ConstantNode>(), compare_type);
                }
            }
        };

        /// Start from large constant
        for (const auto & constant : greater_constants)
        {
            visited.clear();
            findPairs(greater_pairs, constant.node, nullptr, CompareType::equals);
        }

        /// Start from small constant
        for (const auto & constant : less_constants)
        {
            visited.clear();
            findPairs(less_pairs, constant.node, nullptr, CompareType::equals);
        }

        auto and_function_resolver = FunctionFactory::instance().get("and", getContext());
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
            if (equals_functions.size() < settings[Setting::optimize_min_equality_disjunction_chain_length]
                && !expression.node->getResultType()->lowCardinality())
            {
                std::move(equals_functions.begin(), equals_functions.end(), std::back_inserter(or_operands));
                continue;
            }

            bool is_any_nullable = false;
            Tuple args;
            args.reserve(equals_functions.size());
            DataTypes tuple_element_types;
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
                    tuple_element_types.push_back(rhs_literal->getResultType());
                }
                else
                {
                    const auto * lhs_literal = equals_arguments[0]->as<ConstantNode>();
                    assert(lhs_literal);
                    args.push_back(lhs_literal->getValue());
                    tuple_element_types.push_back(lhs_literal->getResultType());
                }
            }

            auto rhs_node = std::make_shared<ConstantNode>(std::move(args), std::make_shared<DataTypeTuple>(std::move(tuple_element_types)));

            auto in_function = std::make_shared<FunctionNode>("in");
            in_function->markAsOperator();

            QueryTreeNodes in_arguments;
            in_arguments.reserve(2);
            in_arguments.push_back(expression.node);
            in_arguments.push_back(std::move(rhs_node));

            in_function->getArguments().getNodes() = std::move(in_arguments);
            in_function->resolveAsFunction(in_function_resolver);

            DataTypePtr result_type = in_function->getResultType();
            const auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(result_type.get());
            if (type_low_cardinality)
                result_type = type_low_cardinality->getDictionaryType();
            /** For `k :: UInt8`, expression `k = 1 OR k = NULL` with result type Nullable(UInt8)
              * is replaced with `k IN (1, NULL)` with result type UInt8.
              * Convert it back to Nullable(UInt8).
              * And for `k :: LowCardinality(UInt8)`, the transformation of `k IN (1, NULL)` results in type LowCardinality(UInt8).
              * Convert it to LowCardinality(Nullable(UInt8)).
              */
            if (is_any_nullable && !result_type->isNullable())
            {
                DataTypePtr new_result_type = std::make_shared<DataTypeNullable>(result_type);
                if (type_low_cardinality)
                {
                    new_result_type = std::make_shared<DataTypeLowCardinality>(new_result_type);
                }
                auto in_function_nullable = createCastFunction(std::move(in_function), std::move(new_result_type), getContext());
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

        const auto & replacement_function = is_lhs_const ? rhs : lhs;

        const FunctionNode * child_function = replacement_function->as<FunctionNode>();
        if (!child_function || !isBooleanFunction(child_function->getFunctionName()))
            return;

        auto function_node_type = function_node.getResultType();

        // if we have something like `function = 0`, we need to add a `NOT` when dropping the `= 0`
        if (constant_value == 0)
        {
            auto not_resolver = FunctionFactory::instance().get("not", getContext());
            const auto not_node = std::make_shared<FunctionNode>("not");
            not_node->markAsOperator();
            auto & arguments = not_node->getArguments().getNodes();
            arguments.reserve(1);
            arguments.push_back(replacement_function);
            not_node->resolveAsFunction(not_resolver->build(not_node->getArgumentColumns()));
            node = not_node;
        }
        else
            node = replacement_function;

        if (!function_node_type->equals(*node->getResultType()))
        {
            /// Result of replacement_function can be low cardinality, while redundant equal
            /// returns UInt8, and this equal can be an argument of external function -
            /// so we want to convert replacement_function to the expected UInt8
            chassert(function_node_type->equals(*removeLowCardinality(node->getResultType())));
            node = createCastFunction(node, function_node_type, getContext());
        }
    }
};

void LogicalExpressionOptimizerPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    LogicalExpressionOptimizerVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
