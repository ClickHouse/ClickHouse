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
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>

#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>


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

/// Collect the bare column names referenced by the custom key expression AST.
NameSet collectCustomKeyColumns(const ASTPtr & custom_key)
{
    NameSet columns;
    for (const auto * identifier : IdentifiersCollector::collect(custom_key))
        columns.insert(identifier->name());
    return columns;
}

}

bool customKeyResultCanSkipMerge(const ASTSelectQuery & select, const ASTPtr & custom_key)
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

    /// Collect bare GROUP BY key column names (only plain column references count).
    NameSet group_by_columns;
    for (const auto & group_by_element : group_by->children)
    {
        if (const auto * identifier = group_by_element->as<ASTIdentifier>())
            group_by_columns.insert(identifier->name());
    }
    if (group_by_columns.empty())
        return false;

    /// The custom key must reference only columns that are themselves GROUP BY keys.
    const NameSet custom_key_columns = collectCustomKeyColumns(custom_key);
    if (custom_key_columns.empty())
        return false;
    for (const auto & column : custom_key_columns)
    {
        if (!group_by_columns.contains(column))
            return false;
    }

    return true;
}

bool customKeyResultCanSkipMerge(const QueryTreeNodePtr & query_tree, const ASTPtr & custom_key)
{
    const auto * query_node = query_tree->as<QueryNode>();
    if (!query_node)
        return false;

    if (query_node->isGroupByWithTotals() || query_node->isGroupByWithRollup() || query_node->isGroupByWithCube()
        || query_node->isGroupByWithGroupingSets())
        return false;

    if (!query_node->hasGroupBy())
        return false;

    /// Collect bare GROUP BY key column names (only plain column references count).
    NameSet group_by_columns;
    for (const auto & group_by_element : query_node->getGroupBy().getNodes())
    {
        if (const auto * column_node = group_by_element->as<ColumnNode>())
            group_by_columns.insert(column_node->getColumnName());
    }
    if (group_by_columns.empty())
        return false;

    /// The custom key AST references bare column names; check they are all GROUP BY keys.
    const NameSet custom_key_columns = collectCustomKeyColumns(custom_key);
    if (custom_key_columns.empty())
        return false;
    for (const auto & column : custom_key_columns)
    {
        if (!group_by_columns.contains(column))
            return false;
    }

    return true;
}

}
