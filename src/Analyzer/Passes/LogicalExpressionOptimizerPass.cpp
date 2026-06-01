#include <Analyzer/Passes/LogicalExpressionOptimizerPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/Utils.h>
#include <Common/FieldAccurateComparison.h>
#include <Core/AccurateComparison.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/convertFieldToType.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 optimize_min_equality_disjunction_chain_length;
    extern const SettingsUInt64 optimize_min_inequality_conjunction_chain_length;
    extern const SettingsBool optimize_extract_common_expressions;
    extern const SettingsBool optimize_and_compare_chain;
    extern const SettingsBool optimize_redundant_comparisons;
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


static bool isBooleanFunction(const String & func_name)
{
    return std::any_of(
        boolean_functions.begin(), boolean_functions.end(), [&](const auto boolean_func) { return func_name == boolean_func; });
}


static QueryTreeNodePtr findEqualsFunction(const QueryTreeNodes & nodes)
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
static bool matchIsNullOfTwoArgs(const QueryTreeNodes & nodes, QueryTreeNodePtr & lhs, QueryTreeNodePtr & rhs)
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

static bool isBooleanConstant(const QueryTreeNodePtr & node, bool expected_value)
{
    const auto * constant_node = node->as<ConstantNode>();
    if (!constant_node || !constant_node->getResultType()->equals(DataTypeUInt8()))
        return false;

    UInt64 constant_value = 0;
    return (constant_node->getValue().tryGet<UInt64>(constant_value) && constant_value == expected_value);
}

/// Returns true if expression consists of only conjunctions of functions with the specified name or true constants
static bool isOnlyConjunctionOfFunctions(
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
static bool isTwoArgumentsFromDifferentSides(const FunctionNode & node_function, const JoinNode & join_node)
{
    const auto & argument_nodes = node_function.getArguments().getNodes();
    if (argument_nodes.size() != 2)
        return false;

    auto first_src = getExpressionSource(argument_nodes[0]).first;
    auto second_src = getExpressionSource(argument_nodes[1]).first;
    if (!first_src || !second_src)
        return false;

    const auto & lhs_join = *join_node.getLeftTableExpression();
    const auto & rhs_join = *join_node.getRightTableExpression();
    return (first_src->isEqual(lhs_join) && second_src->isEqual(rhs_join)) ||
           (first_src->isEqual(rhs_join) && second_src->isEqual(lhs_join));
}

static void insertIfNotPresentInSet(QueryTreeNodePtrWithHashSet& set, QueryTreeNodes &nodes, QueryTreeNodePtr node)
{
    const auto [_, inserted] = set.emplace(node);
    if (inserted)
        nodes.push_back(std::move(node));
}

// Returns the flattened AND/OR node if the passed-in node can be flattened. Doesn't modify the passed-in node.
static std::shared_ptr<FunctionNode> getFlattenedLogicalExpression(const FunctionNode & node, const ContextPtr & context)
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

/// Helper types and functions for comparison chain pruning in `tryOptimizeAndCompareNotEqualsChain`.

/// Comparison pruning groups operands by structural equality of the compared expression, which is
/// only sound for deterministic expressions. Two syntactically identical non-deterministic calls
/// (e.g. `rand() % 2`) are independent evaluations and may differ for the same row, so folding or
/// merging them (e.g. `rand() % 2 < 1 AND rand() % 2 >= 1` → false) would be incorrect. Such
/// expressions must be excluded from pruning, conflict detection and NOT IN conversion.
static bool hasNonDeterministicFunction(const QueryTreeNodePtr & node)
{
    if (const auto * function_node = node->as<FunctionNode>())
    {
        /// Only ordinary functions can be non-deterministic here (e.g. `rand`); aggregate and
        /// window functions are deterministic within the scope of the query.
        if (function_node->isOrdinaryFunction() && !function_node->getFunctionOrThrow()->isDeterministicInScopeOfQuery())
            return true;
    }

    for (const auto & child : node->getChildren())
    {
        if (child && hasNonDeterministicFunction(child))
            return true;
    }

    return false;
}

enum class ComparisonFunction : uint8_t
{
    EQUALS,
    NOT_EQUALS,
    LESS,
    LESS_OR_EQUALS,
    GREATER,
    GREATER_OR_EQUALS
};

static std::optional<ComparisonFunction> toComparisonFunction(const String & name)
{
    if (name == "equals") return ComparisonFunction::EQUALS;
    if (name == "notEquals") return ComparisonFunction::NOT_EQUALS;
    if (name == "less") return ComparisonFunction::LESS;
    if (name == "lessOrEquals") return ComparisonFunction::LESS_OR_EQUALS;
    if (name == "greater") return ComparisonFunction::GREATER;
    if (name == "greaterOrEquals") return ComparisonFunction::GREATER_OR_EQUALS;
    return std::nullopt;
}

static bool isLessThanCompare(ComparisonFunction f)
{
    return f == ComparisonFunction::LESS || f == ComparisonFunction::LESS_OR_EQUALS;
}

static bool isGreaterThanCompare(ComparisonFunction f)
{
    return f == ComparisonFunction::GREATER || f == ComparisonFunction::GREATER_OR_EQUALS;
}

static ComparisonFunction flipComparisonFunction(ComparisonFunction f)
{
    switch (f)
    {
    case ComparisonFunction::LESS:
        return ComparisonFunction::GREATER;
    case ComparisonFunction::GREATER:
        return ComparisonFunction::LESS;
    case ComparisonFunction::LESS_OR_EQUALS:
        return ComparisonFunction::GREATER_OR_EQUALS;
    case ComparisonFunction::GREATER_OR_EQUALS:
        return ComparisonFunction::LESS_OR_EQUALS;
    case ComparisonFunction::EQUALS:
        return ComparisonFunction::EQUALS;
    case ComparisonFunction::NOT_EQUALS:
        return ComparisonFunction::NOT_EQUALS;
    }
    UNREACHABLE();
}

static String comparisonFunctionToName(ComparisonFunction f)
{
    switch (f)
    {
    case ComparisonFunction::EQUALS:
        return "equals";
    case ComparisonFunction::NOT_EQUALS:
        return "notEquals";
    case ComparisonFunction::LESS:
        return "less";
    case ComparisonFunction::LESS_OR_EQUALS:
        return "lessOrEquals";
    case ComparisonFunction::GREATER:
        return "greater";
    case ComparisonFunction::GREATER_OR_EQUALS:
        return "greaterOrEquals";
    }
    UNREACHABLE();
}

static ComparisonFunction strengthenComparison(ComparisonFunction f)
{
    switch (f)
    {
    case ComparisonFunction::LESS_OR_EQUALS:
        return ComparisonFunction::LESS;
    case ComparisonFunction::GREATER_OR_EQUALS:
        return ComparisonFunction::GREATER;
    default:
        UNREACHABLE();
    }
}

/// Result of comparing two filters on the same expression.
enum class ValueComparisonResult
{
    PRUNE_LEFT,        /// The left (existing) filter is weaker and can be removed.
    PRUNE_RIGHT,       /// The right (new) filter is weaker and can be discarded.
    STRENGTHEN_LEFT,   /// Left's inclusive comparison is tightened to strict (e.g. <= → <); right is pruned.
    STRENGTHEN_RIGHT,  /// Right's inclusive comparison is tightened to strict (e.g. <= → <); left is pruned.
    CONFLICT,          /// The two filters are contradictory — the AND is always false.
    NONE               /// Filters are independent, both must be kept.
};

/// Result of inserting a new filter into the per-expression filter list.
enum class AddComparisonFilterResult
{
    CONFLICT,     /// Contradiction detected — the whole AND is false.
    REDUNDANT,    /// The new filter is always true for this column type (boundary folding).
    ADDED         /// The filter was added (possibly after pruning weaker existing filters).
};

/// Per-condition state kept in `ComparisonFilterMap` for one `expr op constant` operand.
struct ComparisonFilterInfo
{
    const ConstantNode * constant_node;
    ComparisonFunction function;
    QueryTreeNodePtr original_node;          /// Original query tree node of this comparison.
    std::optional<Field> converted_value;    /// Constant converted to the column type.
    size_t original_index = 0;               /// Position in the original AND argument list (for stable ordering).
    /// (e.g. for an Int32 column, `i < 3.5` is internally rewritten to `i <= 3`, with
    /// `function = LESS_OR_EQUALS` and `converted_value = 3`).  In that case `original_node`
    /// still references the old literal (3.5) and any later rebuild must substitute it with
    /// `converted_value`, otherwise the AST goes out of sync with the analysis state.
    bool constant_rewritten = false;
};

using ComparisonFilterMap = QueryTreeNodePtrWithHashMap<std::vector<ComparisonFilterInfo>>;

static ValueComparisonResult invertComparisonResult(ValueComparisonResult result)
{
    switch (result)
    {
    case ValueComparisonResult::PRUNE_LEFT:
        return ValueComparisonResult::PRUNE_RIGHT;
    case ValueComparisonResult::PRUNE_RIGHT:
        return ValueComparisonResult::PRUNE_LEFT;
    case ValueComparisonResult::STRENGTHEN_LEFT:
        return ValueComparisonResult::STRENGTHEN_RIGHT;
    case ValueComparisonResult::STRENGTHEN_RIGHT:
        return ValueComparisonResult::STRENGTHEN_LEFT;
    default:
        return result;
    }
}

/// Try to convert a constant to the expression's (column) type using strict (lossless) conversion.
/// Returns the converted Field if successful, or std::nullopt if the conversion is lossy or fails.
static std::optional<Field> tryConvertToColumnType(const ConstantNode * constant_node, const DataTypePtr & expr_type)
{
    try
    {
        const auto & from_type = constant_node->getResultType();

        if (from_type->equals(*expr_type))
            return constant_node->getValue();

        auto converted = convertFieldToType(constant_node->getValue(), *expr_type, from_type.get(), {}, /*strict=*/true);
        if (converted.isNull())
            return std::nullopt;
        return converted;
    }
    catch (const Exception &) /// Ok: conversion failure means we can't optimize, not an error
    {
        return std::nullopt;
    }
}

enum class BoundaryCheckResult : uint8_t
{
    ABOVE_MAX,
    BELOW_MIN,
    AT_MAX,
    AT_MIN,
    IN_RANGE
};

template <typename T>
static BoundaryCheckResult checkBoundaryImpl(const Field & value)
{
    auto check = [](const auto & v) -> BoundaryCheckResult
    {
        if (accurate::greaterOp(v, std::numeric_limits<T>::max()))
            return BoundaryCheckResult::ABOVE_MAX;
        if (accurate::lessOp(v, std::numeric_limits<T>::lowest()))
            return BoundaryCheckResult::BELOW_MIN;
        if (accurate::equalsOp(v, std::numeric_limits<T>::max()))
            return BoundaryCheckResult::AT_MAX;
        if (accurate::equalsOp(v, std::numeric_limits<T>::lowest()))
            return BoundaryCheckResult::AT_MIN;
        return BoundaryCheckResult::IN_RANGE;
    };

    switch (value.getType())
    {
    case Field::Types::UInt64:
        return check(value.safeGet<UInt64>());
    case Field::Types::Int64:
        return check(value.safeGet<Int64>());
    case Field::Types::Float64:
        return check(value.safeGet<Float64>());
    default:
        return BoundaryCheckResult::IN_RANGE;
    }
}

/// Try to optimize a comparison against a native integer column.  Two transformations:
///
/// 1. Boundary folding: when the constant is out of range or at the type boundary, fold the
///    condition to CONFLICT (always false) / REDUNDANT (always true), or tighten the operator
///    (e.g. `Int8_col >= 127` becomes `Int8_col = 127`).
///
/// 2. Float literal rewrite: when the Float64 constant is not exactly representable as the
///    target integer, rewrite it to an equivalent integer predicate via floor/ceil
///    (e.g. `Int32_col > 3.5` becomes `Int32_col >= 4`).
///    For equals/notEquals with non-integer floats, fold directly
///    (e.g. `Int32_col = 3.5` is always false).
///
/// May modify filter.function and filter.converted_value in place.
/// Returns CONFLICT (always false), REDUNDANT (always true), or std::nullopt if no folding occurred.
static std::optional<AddComparisonFilterResult> tryFoldBoundaryOrRewriteFloatForIntColumn(
    ComparisonFilterInfo & filter, const DataTypePtr & column_type)
{
    if (!isNativeInteger(column_type))
        return std::nullopt;

    const bool is_unsigned = column_type->isValueRepresentedByUnsignedInteger();
    const size_t type_size = column_type->getSizeOfValueInMemory();

    auto dispatch_boundary = [&](const Field & value) -> BoundaryCheckResult
    {
        if (is_unsigned)
        {
            switch (type_size)
            {
            case sizeof(UInt8):
                return checkBoundaryImpl<UInt8>(value);
            case sizeof(UInt16):
                return checkBoundaryImpl<UInt16>(value);
            case sizeof(UInt32):
                return checkBoundaryImpl<UInt32>(value);
            case sizeof(UInt64):
                return checkBoundaryImpl<UInt64>(value);
            default:
                UNREACHABLE();
            }
        }
        else
        {
            switch (type_size)
            {
            case sizeof(Int8):
                return checkBoundaryImpl<Int8>(value);
            case sizeof(Int16):
                return checkBoundaryImpl<Int16>(value);
            case sizeof(Int32):
                return checkBoundaryImpl<Int32>(value);
            case sizeof(Int64):
                return checkBoundaryImpl<Int64>(value);
            default:
                UNREACHABLE();
            }
        }
    };

    auto fold_boundary = [&](const Field & value) -> std::optional<AddComparisonFilterResult>
    {
        auto boundary = dispatch_boundary(value);

        if (boundary == BoundaryCheckResult::ABOVE_MAX)
        {
            switch (filter.function)
            {
            case ComparisonFunction::LESS:
            case ComparisonFunction::LESS_OR_EQUALS:
            case ComparisonFunction::NOT_EQUALS:
                return AddComparisonFilterResult::REDUNDANT;
            case ComparisonFunction::GREATER:
            case ComparisonFunction::GREATER_OR_EQUALS:
            case ComparisonFunction::EQUALS:
                return AddComparisonFilterResult::CONFLICT;
            }
            UNREACHABLE();
        }

        if (boundary == BoundaryCheckResult::BELOW_MIN)
        {
            switch (filter.function)
            {
            case ComparisonFunction::GREATER:
            case ComparisonFunction::GREATER_OR_EQUALS:
            case ComparisonFunction::NOT_EQUALS:
                return AddComparisonFilterResult::REDUNDANT;
            case ComparisonFunction::LESS:
            case ComparisonFunction::LESS_OR_EQUALS:
            case ComparisonFunction::EQUALS:
                return AddComparisonFilterResult::CONFLICT;
            }
            UNREACHABLE();
        }

        if (boundary == BoundaryCheckResult::AT_MAX)
        {
            if (filter.function == ComparisonFunction::GREATER)
                return AddComparisonFilterResult::CONFLICT;
            if (filter.function == ComparisonFunction::LESS_OR_EQUALS)
                return AddComparisonFilterResult::REDUNDANT;
            if (filter.function == ComparisonFunction::GREATER_OR_EQUALS)
                filter.function = ComparisonFunction::EQUALS;
        }

        if (boundary == BoundaryCheckResult::AT_MIN)
        {
            if (filter.function == ComparisonFunction::LESS)
                return AddComparisonFilterResult::CONFLICT;
            if (filter.function == ComparisonFunction::GREATER_OR_EQUALS)
                return AddComparisonFilterResult::REDUNDANT;
            if (filter.function == ComparisonFunction::LESS_OR_EQUALS)
                filter.function = ComparisonFunction::EQUALS;
        }

        return std::nullopt;
    };

    const Field & value = filter.converted_value
        ? *filter.converted_value
        : filter.constant_node->getValue();

    if (filter.converted_value || value.getType() != Field::Types::Float64)
        return fold_boundary(value);

    const Float64 float_val = value.safeGet<Float64>();
    if (std::isnan(float_val))
        return std::nullopt;

    auto make_int_field = [&](Float64 v) -> Field
    {
        return is_unsigned ? Field(static_cast<UInt64>(v)) : Field(static_cast<Int64>(v));
    };

    const Float64 floored = std::floor(float_val);
    if (float_val == floored)
    {
        if (auto result = fold_boundary(Field(floored)))
            return result;

        filter.converted_value = make_int_field(floored);
        return fold_boundary(*filter.converted_value);
    }

    switch (filter.function)
    {
    case ComparisonFunction::EQUALS:
        return AddComparisonFilterResult::CONFLICT;
    case ComparisonFunction::NOT_EQUALS:
        return AddComparisonFilterResult::REDUNDANT;
    case ComparisonFunction::LESS:
    case ComparisonFunction::LESS_OR_EQUALS:
    {
        filter.function = ComparisonFunction::LESS_OR_EQUALS;
        if (auto result = fold_boundary(Field(floored)))
            return result;
        filter.converted_value = make_int_field(floored);
        filter.constant_rewritten = true;
        break;
    }
    case ComparisonFunction::GREATER:
    case ComparisonFunction::GREATER_OR_EQUALS:
    {
        filter.function = ComparisonFunction::GREATER_OR_EQUALS;
        const Float64 ceiled = std::ceil(float_val);
        if (auto result = fold_boundary(Field(ceiled)))
            return result;
        filter.converted_value = make_int_field(ceiled);
        filter.constant_rewritten = true;
        break;
    }
    }

    return fold_boundary(*filter.converted_value);
}

/// Determine the relationship between two comparison conditions on the same expression.
/// Returns PRUNE_LEFT/PRUNE_RIGHT when one condition subsumes the other,
/// CONFLICT when they are contradictory, or NONE when both must be kept.
static ValueComparisonResult compareComparisonFilters(const ComparisonFilterInfo & left, const ComparisonFilterInfo & right)
{
    if (!left.converted_value || !right.converted_value)
        return ValueComparisonResult::NONE;

    const auto & lc = *left.converted_value;
    const auto & rc = *right.converted_value;
    const auto lf = left.function;
    const auto rf = right.function;

    try
    {
        /// equals vs anything: check whether the equals value satisfies the other condition.
        /// If yes → PRUNE_RIGHT (e.g. `a = 3 AND a < 5`), if no → CONFLICT (e.g. `a = 3 AND a > 5`).
        if (lf == ComparisonFunction::EQUALS)
        {
            bool prune_right = false;
            switch (rf)
            {
            case ComparisonFunction::LESS:
                prune_right = accurateLess(lc, rc);
                break;
            case ComparisonFunction::LESS_OR_EQUALS:
                prune_right = accurateLessOrEqual(lc, rc);
                break;
            case ComparisonFunction::GREATER:
                prune_right = accurateLess(rc, lc);
                break;
            case ComparisonFunction::GREATER_OR_EQUALS:
                prune_right = accurateLessOrEqual(rc, lc);
                break;
            case ComparisonFunction::NOT_EQUALS:
                prune_right = !accurateEquals(lc, rc);
                break;
            case ComparisonFunction::EQUALS:
                prune_right = accurateEquals(lc, rc);
                break;
            }
            return prune_right ? ValueComparisonResult::PRUNE_RIGHT : ValueComparisonResult::CONFLICT;
        }

        /// Right is equals — swap arguments and invert the result.
        if (rf == ComparisonFunction::EQUALS)
            return invertComparisonResult(compareComparisonFilters(right, left));

        /// notEquals vs range: if the range already excludes the notEquals value,
        /// the notEquals is redundant (e.g. `a != 3 AND a < 3` → PRUNE_LEFT).
        /// When the values are equal and the range is inclusive, strengthen the range
        /// (e.g. `a != 3 AND a <= 3` → `a < 3`, returned as STRENGTHEN_RIGHT).
        /// Otherwise both are independent (e.g. `a != 3 AND a < 5` → NONE).
        /// notEquals vs notEquals: duplicate → PRUNE_RIGHT, otherwise NONE.
        if (lf == ComparisonFunction::NOT_EQUALS)
        {
            bool prune_left = false;
            bool can_strengthen_right = false;
            switch (rf)
            {
            case ComparisonFunction::LESS:
                prune_left = !accurateLess(lc, rc);
                break;
            case ComparisonFunction::LESS_OR_EQUALS:
                prune_left = accurateLess(rc, lc);
                if (!prune_left)
                    can_strengthen_right = accurateEquals(lc, rc);
                break;
            case ComparisonFunction::GREATER:
                prune_left = !accurateLess(rc, lc);
                break;
            case ComparisonFunction::GREATER_OR_EQUALS:
                prune_left = accurateLess(lc, rc);
                if (!prune_left)
                    can_strengthen_right = accurateEquals(lc, rc);
                break;
            case ComparisonFunction::NOT_EQUALS:
                if (accurateEquals(lc, rc))
                    return ValueComparisonResult::PRUNE_RIGHT;
                break;
            case ComparisonFunction::EQUALS:
                UNREACHABLE();
            }
            if (prune_left)
                return ValueComparisonResult::PRUNE_LEFT;
            if (can_strengthen_right)
                return ValueComparisonResult::STRENGTHEN_RIGHT;
            return ValueComparisonResult::NONE;
        }

        /// Right is notEquals — swap arguments and invert the result.
        if (rf == ComparisonFunction::NOT_EQUALS)
            return invertComparisonResult(compareComparisonFilters(right, left));

        /// Same-direction ranges (both > or both <): the tighter bound wins.
        /// E.g. `a > 1 AND a > 3` → PRUNE_LEFT.  When values are equal, < beats <=, > beats >=.
        if (isGreaterThanCompare(lf) && isGreaterThanCompare(rf))
        {
            if (accurateLess(rc, lc))
                return ValueComparisonResult::PRUNE_RIGHT;
            if (accurateLess(lc, rc))
                return ValueComparisonResult::PRUNE_LEFT;
            /// Equal values: > is stricter than >=.
            if (lf == ComparisonFunction::GREATER_OR_EQUALS)
                return ValueComparisonResult::PRUNE_LEFT;
            return ValueComparisonResult::PRUNE_RIGHT;
        }

        /// Same logic for both-less: tighter (smaller) bound wins.
        if (isLessThanCompare(lf) && isLessThanCompare(rf))
        {
            if (accurateLess(lc, rc))
                return ValueComparisonResult::PRUNE_RIGHT;
            if (accurateLess(rc, lc))
                return ValueComparisonResult::PRUNE_LEFT;
            /// Equal values: < is stricter than <=.
            if (lf == ComparisonFunction::LESS_OR_EQUALS)
                return ValueComparisonResult::PRUNE_LEFT;
            return ValueComparisonResult::PRUNE_RIGHT;
        }

        /// Opposite-direction ranges (< vs >): check whether the interval is non-empty.
        /// E.g. `a < 5 AND a > 1` → NONE, `a < 1 AND a > 5` → CONFLICT.
        /// Both inclusive (<= and >=) allows a single-point interval: `a <= 3 AND a >= 3` → NONE.
        if (isLessThanCompare(lf))
        {
            chassert(isGreaterThanCompare(rf));
            bool both_inclusive = (lf == ComparisonFunction::LESS_OR_EQUALS && rf == ComparisonFunction::GREATER_OR_EQUALS);
            if (both_inclusive)
                return accurateLessOrEqual(rc, lc) ? ValueComparisonResult::NONE : ValueComparisonResult::CONFLICT;
            return accurateLess(rc, lc) ? ValueComparisonResult::NONE : ValueComparisonResult::CONFLICT;
        }

        /// Greater vs less — swap to reuse the less-vs-greater branch above.
        chassert(isGreaterThanCompare(lf) && isLessThanCompare(rf));
        return invertComparisonResult(compareComparisonFilters(right, left));
    }
    catch (const Exception &) /// Ok: comparison failure means we can't optimize, not an error
    {
        return ValueComparisonResult::NONE;
    }
}

/// Rebuild the `original_node` of a filter after its `function` has been changed (e.g. by strengthening).
/// Preserves the original argument order.
static void rebuildComparisonNode(ComparisonFilterInfo & filter, const ContextPtr & context)
{
    auto * orig = filter.original_node->as<FunctionNode>();
    chassert(orig);
    auto orig_args = orig->getArguments().getNodes();
    bool constant_on_left = orig_args[0]->as<ConstantNode>() != nullptr;
    auto output_func = constant_on_left ? flipComparisonFunction(filter.function) : filter.function;
    auto func_name = comparisonFunctionToName(output_func);

    QueryTreeNodes new_args = std::move(orig_args);
    if (filter.constant_rewritten)
    {
        chassert(filter.converted_value.has_value());
        const size_t constant_idx = constant_on_left ? 0 : 1;
        new_args[constant_idx] = std::make_shared<ConstantNode>(*filter.converted_value);
    }

    auto new_node = std::make_shared<FunctionNode>(func_name);
    new_node->markAsOperator();
    new_node->getArguments().getNodes() = std::move(new_args);
    resolveOrdinaryFunctionNodeByName(*new_node, func_name, context);
    filter.original_node = std::move(new_node);
}

/// Insert a new comparison filter for `expression` into `filter_map`.
/// When `enable_pruning` is true, performs type conversion, boundary folding, and
/// pairwise comparison against existing filters for the same expression.
/// Returns CONFLICT if a contradiction is found, REDUNDANT if the condition is
/// always true, or ADDED otherwise.
static AddComparisonFilterResult addComparisonFilter(
    ComparisonFilterMap & filter_map,
    const QueryTreeNodePtr & expression,
    ComparisonFilterInfo new_filter,
    bool enable_pruning,
    const ContextPtr & context)
{
    /// Pruning disabled — just store the filter without analysis.
    if (!enable_pruning)
    {
        filter_map[expression].push_back(std::move(new_filter));
        return AddComparisonFilterResult::ADDED;
    }

    /// Step 1: convert the constant to the column's type for uniform comparison.
    const auto & raw_type = expression->getResultType();
    chassert(!raw_type->isNullable());
    auto expr_type = removeLowCardinality(raw_type);

    new_filter.converted_value = tryConvertToColumnType(new_filter.constant_node, expr_type);

    /// Step 2: for integer columns, try boundary folding / float-literal rewriting.
    if (auto result = tryFoldBoundaryOrRewriteFloatForIntColumn(new_filter, expr_type))
        return *result;

    /// First filter for this expression — nothing to compare against.
    auto it = filter_map.find(expression);
    if (it == filter_map.end())
    {
        filter_map[expression].push_back(std::move(new_filter));
        return AddComparisonFilterResult::ADDED;
    }

    /// Step 3: compare pairwise against existing filters for the same expression.
    auto & info_list = it->second;
    for (size_t i = 0; i < info_list.size(); ++i)
    {
        auto result = compareComparisonFilters(info_list[i], new_filter);
        switch (result)
        {
        case ValueComparisonResult::PRUNE_LEFT:
            info_list.erase(info_list.begin() + static_cast<std::ptrdiff_t>(i));
            --i;
            break;
        case ValueComparisonResult::PRUNE_RIGHT:
            return AddComparisonFilterResult::REDUNDANT;
        case ValueComparisonResult::STRENGTHEN_LEFT:
            info_list[i].function = strengthenComparison(info_list[i].function);
            rebuildComparisonNode(info_list[i], context);
            return AddComparisonFilterResult::REDUNDANT;
        case ValueComparisonResult::STRENGTHEN_RIGHT:
            new_filter.function = strengthenComparison(new_filter.function);
            rebuildComparisonNode(new_filter, context);
            info_list.erase(info_list.begin() + static_cast<std::ptrdiff_t>(i));
            --i;
            break;
        case ValueComparisonResult::CONFLICT:
            return AddComparisonFilterResult::CONFLICT;
        case ValueComparisonResult::NONE:
            break;
        }
    }

    info_list.push_back(std::move(new_filter));
    return AddComparisonFilterResult::ADDED;
}

using IndexedNode = std::pair<size_t, QueryTreeNodePtr>;
using IndexedNodes = std::vector<IndexedNode>;

/// Convert surviving notEquals chains to NOT IN when the chain is long enough.
/// Output is appended to `output` with original indices for deterministic sorting.
static void convertNotEqualsChainToNotIn(
    QueryTreeNodePtrWithHashMap<IndexedNodes> & node_to_not_equals_functions,
    IndexedNodes & output,
    const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    auto not_in_function_resolver = FunctionFactory::instance().get("notIn", context);

    for (auto & [expression, not_equals_entries] : node_to_not_equals_functions)
    {
        if (not_equals_entries.size() < settings[Setting::optimize_min_inequality_conjunction_chain_length]
            && !expression.node->getResultType()->lowCardinality())
        {
            std::move(not_equals_entries.begin(), not_equals_entries.end(), std::back_inserter(output));
            continue;
        }

        size_t min_index = not_equals_entries.front().first;
        Tuple args;
        args.reserve(not_equals_entries.size());
        for (auto & [idx, not_equals] : not_equals_entries)
        {
            min_index = std::min(min_index, idx);
            const auto * not_equals_function = not_equals->as<FunctionNode>();
            chassert(not_equals_function && not_equals_function->getFunctionName() == "notEquals");

            const auto & not_equals_arguments = not_equals_function->getArguments().getNodes();
            if (const auto * rhs_literal = not_equals_arguments[1]->as<ConstantNode>())
                args.push_back(rhs_literal->getValue());
            else
            {
                const auto * lhs_literal = not_equals_arguments[0]->as<ConstantNode>();
                chassert(lhs_literal);
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

        output.emplace_back(min_index, std::move(not_in_function));
    }
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
static std::optional<CommonExpressionExtractionResult> tryExtractCommonExpressionsInDisjunction(const QueryTreeNodes & disjuncts, const ContextPtr & context)
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

static std::optional<CommonExpressionExtractionResult> tryExtractCommonExpressions(const QueryTreeNodePtr & node, const ContextPtr & context)
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

static void tryOptimizeCommonExpressionsInOr(QueryTreeNodePtr & node, const ContextPtr & context)
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

        if (new_root_arguments.size() == 1 && new_root_arguments.front()->getResultType()->equals(*node->getResultType()))
        {
            new_root_node = std::move(new_root_arguments.front());
        }
        else
        {
            /// If only one argument remains but its ResultType does not match the original `or`
            /// (e.g. a `Float64` column), leaving it bare may trigger a lossy `_CAST(arg, UInt8)`
            /// below that truncates values like `0.5` to `0` instead of performing `!= 0`. Wrap as
            /// `and(arg, 1)`: `x AND 1` is the boolean identity (semantics preserved), and the AND
            /// function performs the `!= 0` on `arg` internally.
            if (new_root_arguments.size() == 1)
                new_root_arguments.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(1)));

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

static void tryOptimizeCommonExpressionsInAnd(QueryTreeNodePtr & node, const ContextPtr & context)
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

    QueryTreeNodePtr new_root_node;

    if (new_top_level_arguments.size() == 1 && new_top_level_arguments.front()->getResultType()->equals(*node->getResultType()))
    {
        new_root_node = std::move(new_top_level_arguments.front());
    }
    else
    {
        /// If only one argument remains but its ResultType does not match the original `and`
        /// (e.g. a `Float64` column), leaving it bare may trigger a lossy `_CAST(arg, UInt8)`
        /// below that truncates values like `0.5` to `0` instead of performing `!= 0`. Wrap as
        /// `and(arg, 1)`: `x AND 1` is the boolean identity (semantics preserved), and the AND
        /// function performs the `!= 0` on `arg` internally.
        if (new_top_level_arguments.size() == 1)
            new_top_level_arguments.push_back(std::make_shared<ConstantNode>(static_cast<UInt8>(1)));

        auto and_function_node = std::make_shared<FunctionNode>("and");
        and_function_node->markAsOperator();
        and_function_node->getArguments().getNodes() = std::move(new_top_level_arguments);
        auto and_function_resolver = FunctionFactory::instance().get("and", context);
        and_function_node->resolveAsFunction(and_function_resolver);
        new_root_node = std::move(and_function_node);
    }

    if (!new_root_node->getResultType()->equals(*node->getResultType()))
        new_root_node = buildCastFunction(new_root_node, node->getResultType(), context);
    node = std::move(new_root_node);
}

static void tryOptimizeCommonExpressions(QueryTreeNodePtr & node, FunctionNode& function_node, const ContextPtr & context)
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
            /// Run transitive inference first so that the conflict detector can see inferred equalities.
            /// E.g. `x = 3 AND x = y AND y = 5` — the compare-chain pass infers `x = 5`,
            /// then the equals-chain pass detects `x = 3` vs `x = 5` and collapses to FALSE.
            if (getSettings()[Setting::optimize_and_compare_chain])
                tryOptimizeAndCompareChain(node);
            tryOptimizeAndCompareNotEqualsChain(node);
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
    /** Optimize AND chains by analyzing comparison conditions on the same expression.
      * This method performs two things in a single pass:
      *
      * (a) Comparison chain pruning (when `optimize_redundant_comparisons` is enabled):
      *     Given an AND expression where the same column appears in multiple comparisons
      *     against constants (e.g. `a = 3 AND a < 5 AND a > 1`), we collect all conditions
      *     on the same non-constant expression into a per-expression `ComparisonFilterMap`.
      *     For each new condition added via `addComparisonFilter`, we:
      *       1. Convert the constant to the column's type via `tryConvertToColumnType`.
      *       2. For native integer columns, apply boundary folding and float-literal rewriting
      *          (`tryFoldBoundaryOrRewriteFloatForIntColumn`).
      *       3. Compare the new condition pairwise against every existing condition for the
      *          same expression (`compareComparisonFilters`), yielding one of:
      *            - PRUNE_LEFT:  the existing condition is redundant, remove it.
      *            - PRUNE_RIGHT: the new condition is redundant, discard it.
      *            - CONFLICT:    the conditions are contradictory, the whole AND is false.
      *            - NONE:        both conditions are independent, keep both.
      *
      * (b) notEquals → NOT IN conversion (always active):
      *     Chains of `expr != c1 AND expr != c2 AND ...` are merged into `expr NOT IN (c1, c2, ...)`
      *     when the chain is long enough (controlled by `optimize_min_inequality_conjunction_chain_length`).
      *
      * After processing all operands, surviving filters are reassembled into the AND.
      * Non-comparison operands are kept as-is.
      */
    void tryOptimizeAndCompareNotEqualsChain(QueryTreeNodePtr & node)
    {
        auto & function_node = node->as<FunctionNode &>();
        chassert(function_node.getFunctionName() == "and");

        if (function_node.getResultType()->isNullable())
            return;

        const bool enable_pruning = getSettings()[Setting::optimize_redundant_comparisons];

        ComparisonFilterMap filter_map;
        QueryTreeNodePtrWithHashMap<QueryTreeNodeConstRawPtrWithHashSet> not_equals_node_to_constants;
        QueryTreeNodePtrWithHashMap<IndexedNodes> node_to_not_equals_functions;

        /// Pass 1: iterate over AND operands, classify each as comparison-with-constant or other.
        IndexedNodes all_operands;
        size_t argument_index = 0;
        for (const auto & argument : function_node.getArguments())
        {
            auto * argument_function = argument->as<FunctionNode>();
            auto comparison_func = argument_function
                ? toComparisonFunction(argument_function->getFunctionName())
                : std::nullopt;

            /// Not a comparison — keep as-is.
            if (!comparison_func)
            {
                all_operands.emplace_back(argument_index, argument);
                ++argument_index;
                continue;
            }

            /// Identify which side is constant and which is the expression.
            const auto & function_arguments = argument_function->getArguments().getNodes();
            const auto & lhs = function_arguments[0];
            const auto & rhs = function_arguments[1];

            const ConstantNode * constant = nullptr;
            QueryTreeNodePtr expression;
            bool constant_on_left = false;

            if (const auto * lhs_literal = lhs->as<ConstantNode>())
            {
                chassert(!lhs_literal->getValue().isNull());
                constant = lhs_literal;
                expression = rhs;
                constant_on_left = true;
            }
            else if (const auto * rhs_literal = rhs->as<ConstantNode>())
            {
                chassert(!rhs_literal->getValue().isNull());
                constant = rhs_literal;
                expression = lhs;
            }

            /// Both sides are non-constant — keep as-is.
            if (!constant)
            {
                all_operands.emplace_back(argument_index, argument);
                ++argument_index;
                continue;
            }

            /// Non-deterministic expressions must not be pruned/merged: structurally identical
            /// occurrences (e.g. `rand() % 2`) are independent evaluations, so folding them
            /// (e.g. `rand() % 2 < 1 AND rand() % 2 >= 1` → false) or merging notEquals into NOT IN
            /// would change results. Keep such operands as-is.
            if (hasNonDeterministicFunction(expression))
            {
                all_operands.emplace_back(argument_index, argument);
                ++argument_index;
                continue;
            }

            /// Normalize so that expression is always on the left (e.g. `3 < a` → `a > 3`).
            auto normalized = constant_on_left ? flipComparisonFunction(*comparison_func) : *comparison_func;
            ComparisonFilterInfo new_filter{constant, normalized, argument, {}, argument_index};
            auto result = addComparisonFilter(filter_map, expression, std::move(new_filter), enable_pruning, getContext());

            /// Contradiction detected — the entire AND is false.
            if (result == AddComparisonFilterResult::CONFLICT)
            {
                auto false_node = std::make_shared<ConstantNode>(0u, function_node.getResultType());
                node = std::move(false_node);
                return;
            }

            ++argument_index;
        }

        /// Pass 2: collect surviving filters, separating notEquals for NOT IN conversion.
        for (auto & [expression, filters] : filter_map)
        {
            for (auto & filter : filters)
            {
                if (filter.function == ComparisonFunction::NOT_EQUALS)
                {
                    auto & constant_set = not_equals_node_to_constants[expression];
                    if (!constant_set.contains(filter.constant_node))
                    {
                        constant_set.insert(filter.constant_node);
                        node_to_not_equals_functions[expression].emplace_back(filter.original_index, std::move(filter.original_node));
                    }
                }
                else
                {
                    all_operands.emplace_back(filter.original_index, std::move(filter.original_node));
                }
            }
        }

        /// Merge notEquals chains into NOT IN where possible.
        convertNotEqualsChainToNotIn(node_to_not_equals_functions, all_operands, getContext());

        /// Reassemble: sort by original index to preserve the original operand order.
        std::sort(all_operands.begin(), all_operands.end(),
            [](const auto & a, const auto & b) { return a.first < b.first; });

        QueryTreeNodes and_operands;
        and_operands.reserve(all_operands.size());
        for (auto & [_, node_ptr] : all_operands)
            and_operands.push_back(std::move(node_ptr));

        if (and_operands.size() == function_node.getArguments().getNodes().size())
            return;

        /// All conditions were redundant — replace with true.
        if (and_operands.empty())
        {
            node = std::make_shared<ConstantNode>(1u, function_node.getResultType());
            return;
        }

        if (and_operands.size() == 1)
        {
            /// AND operator can have UInt8 or bool as its type.
            /// bool is used if a bool constant is at least one operand.

            auto operand_type = and_operands[0]->getResultType();
            auto function_type = function_node.getResultType();
            chassert(!function_type->isNullable());

            /// The sole remaining operand may be a bare numeric constant (e.g. `256`).
            /// A plain cast to UInt8 would silently truncate (e.g. 256 → 0),
            /// so evaluate it as a boolean by checking for non-zero instead.
            if (const auto * constant = and_operands[0]->as<ConstantNode>())
            {
                const auto & field = constant->getValue();
                auto ft = field.getType();
                if (ft == Field::Types::Float64)
                {
                    node = std::make_shared<ConstantNode>(static_cast<UInt8>(field.safeGet<Float64>() != 0), function_type);
                    return;
                }
                if (ft == Field::Types::Int64)
                {
                    node = std::make_shared<ConstantNode>(static_cast<UInt8>(field.safeGet<Int64>() != 0), function_type);
                    return;
                }
                if (ft == Field::Types::UInt64)
                {
                    node = std::make_shared<ConstantNode>(static_cast<UInt8>(field.safeGet<UInt64>() != 0), function_type);
                    return;
                }
            }

            if (!function_type->equals(*operand_type))
            {
                /// Result of equality operator can be low cardinality, while AND always returns UInt8.
                /// In that case we replace `(lc = 1) AND (lc = 1)` with `(lc = 1) AS UInt8`
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

            /// Skip comparisons whose operands contain a non-deterministic function: transitive
            /// inference would treat independent evaluations (e.g. two `rand()` calls) as the same
            /// value and could infer false facts (e.g. from `rand() % 2 = number % 2 AND rand() % 2 = 1`
            /// it would derive `number % 2 = 1`), which the conflict detector then folds away.
            if (hasNonDeterministicFunction(lhs) || hasNonDeterministicFunction(rhs))
                continue;

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
        std::function<void(const ComparePairs &, QueryTreeNodePtr, const ConstantNode *, CompareType)> find_pairs
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

                    /// Don't recurse through constant intermediate nodes — any inference would be
                    /// redundant. E.g. `x < 3 AND y > 3 AND y < 10` builds the chain
                    /// 10 > y > 3 > x, and chaining through constant 3 would add `x < 10`
                    /// which is strictly weaker than the existing `x < 3`.
                    if (!left.first->as<ConstantNode>())
                        find_pairs(pairs, left.first, constant ? constant : current->as<ConstantNode>(), compare_type);
                }
            }
        };

        /// Start from large constant
        for (const auto & constant : greater_constants)
        {
            visited.clear();
            find_pairs(greater_pairs, constant.node, nullptr, CompareType::equals);
        }

        /// Start from small constant
        for (const auto & constant : less_constants)
        {
            visited.clear();
            find_pairs(less_pairs, constant.node, nullptr, CompareType::equals);
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
                chassert(equals_function && equals_function->getFunctionName() == "equals");

                const auto & equals_arguments = equals_function->getArguments().getNodes();
                if (const auto * rhs_literal = equals_arguments[1]->as<ConstantNode>())
                {
                    args.push_back(rhs_literal->getValue());
                    tuple_element_types.push_back(rhs_literal->getResultType());
                }
                else
                {
                    const auto * lhs_literal = equals_arguments[0]->as<ConstantNode>();
                    chassert(lhs_literal);
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
        chassert(function_node.getFunctionName() == "equals");

        const auto function_arguments = function_node.getArguments().getNodes();
        if (function_arguments.size() != 2)
            return;

        const auto & lhs = function_arguments[0];
        const auto & rhs = function_arguments[1];

        UInt64 constant_value = 0;
        bool is_lhs_const = false;
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
        auto original_node = node;

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
            /// Result of replacement_function can be low cardinality or nullable, while redundant equal
            /// returns UInt8, and this equal can be an argument of external function -
            /// so we want to convert replacement_function to the expected UInt8
            /// An example when it can be Nullable - using GROUP BY with GROUPING SETS and group_by_use_nulls = true
            if (!removeNullable(removeLowCardinality(function_node_type))->equals(*removeNullable(removeLowCardinality(node->getResultType()))))
            {
                /// Types differ beyond Nullable/LowCardinality wrappers (e.g. Variant types),
                /// bail out and keep the original equals expression.
                node = original_node;
                return;
            }
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
