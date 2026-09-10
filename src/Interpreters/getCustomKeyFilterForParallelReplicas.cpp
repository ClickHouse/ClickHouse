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
    extern const int BAD_ARGUMENTS;
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

    /// The custom key is an arbitrary expression, and it does not have to describe a single column.
    /// `tuple()` describes no columns at all, so the number of the types has to be checked before reading them.
    if (custom_key_description.data_types.size() != 1)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid custom key expression: it describes {} columns. Must be one unsigned integer column",
            custom_key_description.data_types.size());

    DataTypePtr custom_key_column_type = custom_key_description.data_types[0];

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
    /// The callers get here only when the custom key filtering is requested. Every replica reads only the part of
    /// the data its filter selects, so without the key every replica would read everything and the result would be
    /// multiplied by the number of the replicas. Fail instead, the same way for every caller.
    if (custom_key.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The custom key filtering for the parallel replicas is requested (setting 'parallel_replicas_mode'), "
            "but the custom key itself is not set (setting 'parallel_replicas_custom_key')");

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

/// Safe means the function maps a group's key values to the same replica offset on every replica. None of
/// the four flags implies another: `timeSeriesStoreTags` is deterministic but stateful, and `__getScalar`
/// is neither non-deterministic nor stateful yet is resolved per server.
template <typename FunctionOrResolver>
bool isSafeCustomKeyFunction(const FunctionOrResolver & function)
{
    return function.isDeterministic() && function.isDeterministicInScopeOfQuery() && !function.isStateful()
        && !function.isServerConstant();
}

/// Matching is by tree hash, so expression keys like `mod(number, 3)` are handled and not only bare
/// columns. Comparing unresolved ASTs is conservative: a differently spelled alias on the two sides means
/// the fast path is not taken, never that it is taken unsafely.
bool containsUnsafeFunctionAST(const ASTPtr & node, const Context & context);

bool isDeterministicFunctionOfKeys(const ASTPtr & node, const std::vector<IASTHash> & key_hashes, const Context & context)
{
    /// Equality with a GROUP BY key is not on its own sufficient: the replica filter and the grouping are
    /// evaluated independently on each replica, so grouping by an unsafe expression does not make that
    /// expression safe to partition on (`GROUP BY getMacro('replica')` with the same custom key still
    /// spreads one group over several replicas).
    if (containsHash(key_hashes, node->getTreeHash(/*ignore_aliases=*/true)))
        return !containsUnsafeFunctionAST(node, context);

    if (node->as<ASTLiteral>())
        return true;

    if (const auto * function = node->as<ASTFunction>())
    {
        /// Aggregate/window/lambda and unknown functions are not ordinary functions of their arguments.
        const auto resolver = FunctionFactory::instance().tryGet(function->name, context.shared_from_this());
        if (!resolver)
            return false;
        if (!isSafeCustomKeyFunction(*resolver))
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

/// Returns true if the AST contains any function that is not safe for the no-merge fast path, including
/// unknown functions (which cannot be proven safe).
bool containsUnsafeFunctionAST(const ASTPtr & node, const Context & context)
{
    if (const auto * function = node->as<ASTFunction>())
    {
        const auto resolver = FunctionFactory::instance().tryGet(function->name, context.shared_from_this());
        if (!resolver || !isSafeCustomKeyFunction(*resolver))
            return true;
    }

    for (const auto & child : node->children)
    {
        if (child && containsUnsafeFunctionAST(child, context))
            return true;
    }

    return false;
}

/// `isExpressionFunctionOfKeys` rejects only isDeterministicInScopeOfQuery() == false, which lets through
/// stateful functions and server constants, so apply the same predicate the AST path uses.
bool containsUnsafeFunction(const QueryTreeNodePtr & node)
{
    if (const auto * function = node->as<FunctionNode>())
    {
        /// Aggregate and window function nodes hold no ordinary function, and such a custom key is
        /// rejected anyway because it is not a function of the keys.
        const auto function_base = function->getFunction();
        if (!function_base || !isSafeCustomKeyFunction(*function_base))
            return true;
    }

    for (const auto & child : node->getChildren())
    {
        if (child && containsUnsafeFunction(child))
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
    /// while a custom key whose value is not determined by the group's keys (e.g. `y + rand()`) is
    /// rejected below.
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

    /// `isExpressionFunctionOfKeys` only rejects functions with isDeterministicInScopeOfQuery() == false,
    /// so stateful functions (`timeSeriesTagsToGroup`) and server constants (`getMacro()`) would still be
    /// accepted even though their value differs per replica for the same group. Reject them here.
    if (containsUnsafeFunction(custom_key_tree))
        return false;

    /// The custom key itself may be one of the GROUP BY keys (bare column custom key), or a
    /// deterministic function of them.
    return key_set.contains(custom_key_tree) || isExpressionFunctionOfKeys(custom_key_tree, key_set);
}

}
