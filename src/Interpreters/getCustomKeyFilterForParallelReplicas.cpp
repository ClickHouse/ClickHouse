#include <Interpreters/getCustomKeyFilterForParallelReplicas.h>
#include <DataTypes/DataTypesNumber.h>

#include <Core/Settings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>

#include <algorithm>

#include <boost/rational.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int INVALID_SETTING_VALUE;
}

ASTPtr getCustomKeyFilterForParallelReplica(
    size_t replicas_count,
    size_t replica_num,
    ASTPtr custom_key_ast,
    ParallelReplicasCustomKeyFilter filter,
    const ColumnsDescription & columns,
    const ContextPtr & context)
{
    chassert(replicas_count > 1);
    chassert(filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_SAMPLING || filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_RANGE);
    if (filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_SAMPLING)
    {
        // first we do modulo with replica count
        auto modulo_function = makeASTFunction("positiveModulo", custom_key_ast, make_intrusive<ASTLiteral>(replicas_count));

        /// then we compare result to the current replica number (offset)
        auto equals_function = makeASTOperator("equals", std::move(modulo_function), make_intrusive<ASTLiteral>(replica_num));

        return equals_function;
    }

    chassert(filter.filter_type == ParallelReplicasMode::CUSTOM_KEY_RANGE);

    KeyDescription custom_key_description = KeyDescription::getKeyFromAST(custom_key_ast, columns, {}, context);

    using RelativeSize = boost::rational<ASTSampleRatio::BigNum>;

    RelativeSize range_upper = RelativeSize(0);
    RelativeSize range_lower = RelativeSize(filter.range_lower);
    DataTypePtr custom_key_column_type = custom_key_description.data_types[0];

    if (custom_key_description.data_types.size() == 1)
    {
        if (typeid_cast<const DataTypeUInt64 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt64) max value",
                    rational_cast<double>(range_upper));
        }
        else if (typeid_cast<const DataTypeUInt32 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt32) max value",
                    rational_cast<double>(range_upper));
        }
        else if (typeid_cast<const DataTypeUInt16 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt16) max value",
                    rational_cast<double>(range_upper));
        }
        else if (typeid_cast<const DataTypeUInt8 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt8) max value",
                    rational_cast<double>(range_upper));
        }
    }

    if (range_upper == RelativeSize(0))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid custom key column type: {}. Must be one unsigned integer type",
            custom_key_column_type->getName());

    if (range_lower >= range_upper)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid custom key filter range: Range upper bound {} must be larger than range lower bound {}",
            rational_cast<double>(range_lower),
            rational_cast<double>(range_upper));

    RelativeSize size_of_universum = range_upper - range_lower;

    if (size_of_universum <= RelativeSize(replicas_count))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE, "Invalid custom key filter range: Range must be larger than than the number of replicas");

    RelativeSize relative_range_size = RelativeSize(1) / replicas_count;
    RelativeSize relative_range_offset = relative_range_size * RelativeSize(replica_num);

    /// Calculate the half-interval of `[lower, upper)` column values.
    bool has_lower_limit = false;
    bool has_upper_limit = false;

    RelativeSize lower_limit_rational = range_lower + relative_range_offset * size_of_universum;
    RelativeSize upper_limit_rational = range_lower + (relative_range_offset + relative_range_size) * size_of_universum;

    UInt64 lower = static_cast<UInt64>(boost::rational_cast<ASTSampleRatio::BigNum>(lower_limit_rational));
    UInt64 upper = static_cast<UInt64>(boost::rational_cast<ASTSampleRatio::BigNum>(upper_limit_rational));

    if (lower_limit_rational > range_lower)
        has_lower_limit = true;

    if (upper_limit_rational < range_upper)
        has_upper_limit = true;

    chassert(has_lower_limit || has_upper_limit);

    /// Let's add the conditions to cut off something else when the index is scanned again and when the request is processed.
    boost::intrusive_ptr<ASTFunction> lower_function;
    boost::intrusive_ptr<ASTFunction> upper_function;

    if (has_lower_limit)
    {
        lower_function = makeASTOperator("greaterOrEquals", custom_key_ast, make_intrusive<ASTLiteral>(lower));

        if (!has_upper_limit)
            return lower_function;
    }

    if (has_upper_limit)
    {
        upper_function = makeASTOperator("less", custom_key_ast, make_intrusive<ASTLiteral>(upper));

        if (!has_lower_limit)
            return upper_function;
    }

    chassert(upper_function && lower_function);

    return makeASTOperator("and", std::move(lower_function), std::move(upper_function));
}

ASTPtr parseCustomKeyForTable(const String & custom_key, const Context & context)
{
    /// Try to parse expression
    ParserExpression parser;
    const auto & settings = context.getSettingsRef();
    return parseQuery(
        parser,
        custom_key.data(),
        custom_key.data() + custom_key.size(),
        "parallel replicas custom key",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
}

namespace
{

bool containsHash(const std::vector<IASTHash> & hashes, const IASTHash & hash)
{
    return std::find(hashes.begin(), hashes.end(), hash) != hashes.end();
}

/// Returns true if `node` is a deterministic function of the given GROUP BY key expressions, i.e. its
/// value is fully determined by the group's key values. This is exactly what the no-merge fast path
/// needs: replica assignment is positiveModulo(custom_key, n) (or a range over custom_key), so every
/// row that shares a GROUP BY key must map to the same replica.
///
/// A subexpression is determined by the keys when it either equals one of the GROUP BY key expressions
/// (matched by tree hash, so expression keys like `mod(number, 3)` are handled, not only bare columns),
/// is a constant, or is a deterministic non-stateful function whose arguments are all determined by the
/// keys. Non-deterministic (`rand()`) or stateful functions are rejected even when they reference only
/// GROUP BY columns, because they scatter rows of the same group across replicas.
///
/// The check is conservative: it operates on the unresolved custom-key AST and compares against the
/// GROUP BY expressions as ASTs, so if a function alias is spelled differently on the two sides the
/// fast path is simply not taken (correct, just an unused optimization) rather than taken unsafely.
bool isDeterministicFunctionOfKeys(const ASTPtr & node, const std::vector<IASTHash> & key_hashes, const Context & context)
{
    if (containsHash(key_hashes, node->getTreeHash(/*ignore_aliases=*/true)))
        return true;

    if (node->as<ASTLiteral>())
        return true;

    if (const auto * function = node->as<ASTFunction>())
    {
        /// Aggregate/window/lambda and unknown functions are not ordinary functions of their arguments.
        const auto resolver = FunctionFactory::instance().tryGet(function->name, context.shared_from_this());
        if (!resolver)
            return false;
        if (!resolver->isDeterministicInScopeOfQuery() || resolver->isStateful())
            return false;

        if (!function->arguments || function->arguments->children.empty())
            return false;
        for (const auto & argument : function->arguments->children)
        {
            if (!isDeterministicFunctionOfKeys(argument, key_hashes, context))
                return false;
        }
        return true;
    }

    /// A bare identifier (or any other node) that does not itself equal a GROUP BY key is not
    /// determined by the keys.
    return false;
}

/// Returns true if the resolved expression tree contains any stateful function. `isExpressionFunctionOfKeys`
/// only rejects non-deterministic functions (isDeterministicInScopeOfQuery() == false); a stateful function
/// such as `timeSeriesTagsToGroup` is marked deterministic-in-scope yet its result depends on a per-query,
/// per-replica collector, so two replicas can assign different custom-key values to the same group. Such a
/// key must not take the no-merge fast path.
bool containsStatefulFunction(const QueryTreeNodePtr & node)
{
    if (const auto * function = node->as<FunctionNode>())
    {
        const auto function_base = function->getFunction();
        if (function_base && function_base->isStateful())
            return true;
    }

    for (const auto & child : node->getChildren())
    {
        if (child && containsStatefulFunction(child))
            return true;
    }

    return false;
}

}

bool customKeyResultCanSkipMerge(const ASTSelectQuery & select, const ASTPtr & custom_key, const Context & context)
{
    /// Concatenating per-replica results without merging is only correct when each GROUP BY key is
    /// fully processed by a single replica. That holds when the custom key is a function of the GROUP BY
    /// keys. GROUP BY modifiers produce extra rows (totals/subtotals) that must be merged on the initiator.
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube
        || select.group_by_with_grouping_sets)
        return false;

    const ASTPtr group_by = select.groupBy();
    if (!group_by || group_by->children.empty())
        return false;

    std::vector<IASTHash> key_hashes;
    key_hashes.reserve(group_by->children.size());
    for (const auto & group_by_element : group_by->children)
        key_hashes.push_back(group_by_element->getTreeHash(/*ignore_aliases=*/true));

    return isDeterministicFunctionOfKeys(custom_key, key_hashes, context);
}

bool customKeyResultCanSkipMerge(const QueryTreeNodePtr & query_tree, const ASTPtr & custom_key, const Context & context)
{
    const auto * query_node = query_tree->as<QueryNode>();
    if (!query_node)
        return false;

    if (query_node->isGroupByWithTotals() || query_node->isGroupByWithRollup() || query_node->isGroupByWithCube()
        || query_node->isGroupByWithGroupingSets())
        return false;

    if (!query_node->hasGroupBy())
        return false;

    /// Resolve the custom key against the query's join tree so its column references and function
    /// names are canonicalized exactly like the resolved GROUP BY expressions. This lets expression
    /// GROUP BY keys (e.g. `GROUP BY mod(number, 3)` with a custom key over `mod(number, 3)`) match,
    /// while `isExpressionFunctionOfKeys` rejects non-deterministic/stateful custom keys (e.g.
    /// `y + rand()`) because their value is not determined by the group's keys.
    QueryTreeNodePtr custom_key_tree;
    try
    {
        custom_key_tree = buildQueryTree(custom_key, context.shared_from_this());
        QueryAnalysisPass query_analysis_pass(query_node->getJoinTree(), /*only_analyze_=*/true);
        query_analysis_pass.run(custom_key_tree, context.shared_from_this());
    }
    catch (...)
    {
        /// Ok to swallow: if the custom key cannot be resolved against the query (unknown column,
        /// ambiguous name, etc.), conservatively keep the merge rather than risk wrong results.
        return false;
    }

    QueryTreeNodePtrWithHashSet key_set(query_node->getGroupBy().getNodes().begin(), query_node->getGroupBy().getNodes().end());
    if (key_set.empty())
        return false;

    /// `isExpressionFunctionOfKeys` rejects non-deterministic functions but not stateful ones (e.g.
    /// `timeSeriesTagsToGroup`), which produce replica-local values for the same group. Reject them here.
    if (containsStatefulFunction(custom_key_tree))
        return false;

    /// The custom key itself may be one of the GROUP BY keys (bare column custom key), or a
    /// deterministic function of them.
    return key_set.contains(custom_key_tree) || isExpressionFunctionOfKeys(custom_key_tree, key_set);
}

}
