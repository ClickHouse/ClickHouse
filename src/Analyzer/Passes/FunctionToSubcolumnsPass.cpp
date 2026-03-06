#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeObject.h>

#include <Storages/IStorage.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <IO/WriteHelpers.h>

#include <stack>


namespace DB
{

namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool join_use_nulls;
    extern const SettingsBool optimize_functions_to_subcolumns;
}

namespace
{

struct ColumnContext
{
    NameAndTypePair column;
    QueryTreeNodePtr column_source;
    ContextPtr context;
};

using NodeToSubcolumnTransformer = std::function<void(QueryTreeNodePtr &, FunctionNode &, ColumnContext &)>;

/// Before columns to substream optimization, we need to make sure, that column with such name as substream does not exists, otherwise the optimize will use it instead of substream.
bool sourceHasColumn(QueryTreeNodePtr column_source, const String & column_name)
{
    auto * table_node = column_source->as<TableNode>();
    if (!table_node)
        return {};

    const auto & storage_snapshot = table_node->getStorageSnapshot();
    return storage_snapshot->tryGetColumn(GetColumnsOptions::All, column_name).has_value();
}

/// Sometimes we cannot optimize function to subcolumn because there is no such subcolumn in the table.
/// For example, for column "a Array(Tuple(b UInt32))" function length(a.b) cannot be replaced to
/// a.b.size0, because there is no such subcolumn, even though a.b has type Array(UInt32)
bool canOptimizeToSubcolumn(QueryTreeNodePtr column_source, const String & subcolumn_name)
{
    auto * table_node = column_source->as<TableNode>();
    if (!table_node)
        return {};

    const auto & storage_snapshot = table_node->getStorageSnapshot();
    return storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), subcolumn_name).has_value();
}

void optimizeFunctionStringLength(QueryTreeNodePtr & node, FunctionNode &, ColumnContext & ctx)
{
    /// Replace `length(argument)` with `argument.size`.
    /// `argument` is String.

    NameAndTypePair column{ctx.column.name + ".size", std::make_shared<DataTypeUInt64>()};
    if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
        return;
    node = std::make_shared<ColumnNode>(column, ctx.column_source);
}

template <bool positive>
void optimizeFunctionStringEmpty(QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
{
    /// Replace `empty(argument)` with `equals(argument.size, 0)` if positive.
    /// Replace `notEmpty(argument)` with `notEquals(argument.size, 0)` if not positive.
    /// `argument` is String.

    NameAndTypePair column{ctx.column.name + ".size", std::make_shared<DataTypeUInt64>()};
    if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
        return;
    auto & function_arguments_nodes = function_node.getArguments().getNodes();

    function_arguments_nodes.clear();
    function_arguments_nodes.push_back(std::make_shared<ColumnNode>(column, ctx.column_source));
    function_arguments_nodes.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

    const auto * function_name = positive ? "equals" : "notEquals";
    resolveOrdinaryFunctionNodeByName(function_node, function_name, ctx.context);
}

void optimizeFunctionLength(QueryTreeNodePtr & node, FunctionNode &, ColumnContext & ctx)
{
    /// Replace `length(argument)` with `argument.size0`.
    /// `argument` may be Array or Map.

    NameAndTypePair column{ctx.column.name + ".size0", std::make_shared<DataTypeUInt64>()};
    if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
        return;

    node = std::make_shared<ColumnNode>(column, ctx.column_source);
}

template <bool positive>
void optimizeFunctionEmpty(QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
{
    /// Replace `empty(argument)` with `equals(argument.size0, 0)` if positive.
    /// Replace `notEmpty(argument)` with `notEquals(argument.size0, 0)` if not positive.
    /// `argument` may be Array or Map.

    NameAndTypePair column{ctx.column.name + ".size0", std::make_shared<DataTypeUInt64>()};
    if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
        return;

    /// If the .size0 subcolumn is actually Nullable (e.g. when the column type is Nullable(Array(...))),
    /// skip the optimization. The hardcoded UInt64 type would mismatch the actual Nullable(UInt64),
    /// causing a type mismatch exception at runtime in ExpressionActions::execute.
    if (auto * table_node = ctx.column_source->as<TableNode>())
    {
        auto actual = table_node->getStorageSnapshot()->tryGetColumn(
            GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), column.name);
        if (actual && actual->type->isNullable())
            return;
    }

    auto & function_arguments_nodes = function_node.getArguments().getNodes();

    function_arguments_nodes.clear();
    function_arguments_nodes.push_back(std::make_shared<ColumnNode>(column, ctx.column_source));
    function_arguments_nodes.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

    const auto * function_name = positive ? "equals" : "notEquals";
    function_node.markAsOperator();
    resolveOrdinaryFunctionNodeByName(function_node, function_name, ctx.context);
}

std::optional<NameAndTypePair> getSubcolumnForElement(const Field & value, const DataTypeTuple & data_type_tuple)
{
    const auto & names = data_type_tuple.getElementNames();
    const auto & types = data_type_tuple.getElements();

    if (value.getType() == Field::Types::String)
    {
        const auto & name = value.safeGet<String>();
        auto pos = data_type_tuple.tryGetPositionByName(name);

        if (!pos)
            return {};

        return NameAndTypePair{name, types[*pos]};
    }

    if (value.getType() == Field::Types::UInt64)
    {
        size_t index = value.safeGet<UInt64>();

        if (index == 0 || index > types.size())
            return {};

        return NameAndTypePair{names[index - 1], types[index - 1]};
    }

    /// Maybe negative index
    if (value.getType() == Field::Types::Int64)
    {
        ssize_t index = value.safeGet<Int64>();
        ssize_t size = types.size();

        if (index == 0 || std::abs(index) > size)
            return {};

        if (index > 0)
            return NameAndTypePair{names[index - 1], types[index - 1]};
        else
            return NameAndTypePair{names[size + index], types[size + index]};
    }

    return {};
}

std::optional<NameAndTypePair> getSubcolumnForElement(const Field & value, const DataTypeVariant & data_type_variant)
{
    if (value.getType() != Field::Types::String)
        return {};

    const auto & name = value.safeGet<String>();
    auto discr = data_type_variant.tryGetVariantDiscriminator(name);

    if (!discr)
        return {};

    return NameAndTypePair{name, data_type_variant.getVariant(*discr)};
}

std::optional<NameAndTypePair> getSubcolumnForElement(const Field & value, const DataTypeQBit & data_type_qbit)
{
    size_t index;

    if (value.getType() == Field::Types::UInt64)
        index = value.safeGet<UInt64>();
    else
        return {};

    if (index == 0 || index > data_type_qbit.getElementSize())
        return {};

    return NameAndTypePair{toString(index), std::make_shared<const DataTypeFixedString>((data_type_qbit.getDimension() + 7) / 8)};
}

template <typename DataType>
void optimizeTupleOrVariantElement(QueryTreeNodePtr & node, FunctionNode & function_node, ColumnContext & ctx)
{
    /// Replace `tupleElement(tuple_argument, string_literal)`, `tupleElement(tuple_argument, integer_literal)` with `tuple_argument.column_name`.
    /// Replace `variantElement(variant_argument, string_literal)` with `variant_argument.column_name`.

    auto & function_arguments_nodes = function_node.getArguments().getNodes();
    if (function_arguments_nodes.size() != 2)
        return;

    const auto * second_argument_constant_node = function_arguments_nodes[1]->as<ConstantNode>();
    if (!second_argument_constant_node)
        return;

    const auto & data_type_concrete = assert_cast<const DataType &>(*ctx.column.type);
    auto subcolumn = getSubcolumnForElement(second_argument_constant_node->getValue(), data_type_concrete);

    if (!subcolumn)
        return;

    NameAndTypePair column{ctx.column.name + "." + subcolumn->name, subcolumn->type};
    if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
        return;
    node = std::make_shared<ColumnNode>(column, ctx.column_source);
}

void optimizeDistinctJSONPaths(QueryTreeNodePtr & node, FunctionNode &, ColumnContext & ctx)
{
    /// Replace distinctJSONPaths(json) to arraySort(groupArrayDistinct(arrayJoin(json.__special_subcolumn_name_for_distinct_paths_calculation)))
    NameAndTypePair column{ctx.column.name + "." + DataTypeObject::SPECIAL_SUBCOLUMN_NAME_FOR_DISTINCT_PATHS_CALCULATION, std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())};

    auto new_column_node = std::make_shared<ColumnNode>(column, ctx.column_source);
    auto function_array_join_node = std::make_shared<FunctionNode>("arrayJoin");
    function_array_join_node->getArguments().getNodes().push_back(std::move(new_column_node));
    resolveOrdinaryFunctionNodeByName(*function_array_join_node, "arrayJoin", ctx.context);

    auto function_group_array_distinct_node = std::make_shared<FunctionNode>("groupArrayDistinct");
    function_group_array_distinct_node->getArguments().getNodes().push_back(std::move(function_array_join_node));
    resolveAggregateFunctionNodeByName(*function_group_array_distinct_node, "groupArrayDistinct");

    auto function_array_sort_node = std::make_shared<FunctionNode>("arraySort");
    function_array_sort_node->getArguments().getNodes().push_back(std::move(function_group_array_distinct_node));
    resolveOrdinaryFunctionNodeByName(*function_array_sort_node, "arraySort", ctx.context);

    node = std::move(function_array_sort_node);
}

std::map<std::pair<TypeIndex, String>, NodeToSubcolumnTransformer> node_transformers =
{
    {
        {TypeIndex::String, "length"}, optimizeFunctionStringLength,
    },
    {
        {TypeIndex::String, "empty"}, optimizeFunctionStringEmpty<true>,
    },
    {
        {TypeIndex::String, "notEmpty"}, optimizeFunctionStringEmpty<false>,
    },
    {
        {TypeIndex::Array, "length"}, optimizeFunctionLength,
    },
    {
        {TypeIndex::Array, "empty"}, optimizeFunctionEmpty<true>,
    },
    {
        {TypeIndex::Array, "notEmpty"}, optimizeFunctionEmpty<false>,
    },
    {
        {TypeIndex::Map, "length"}, optimizeFunctionLength,
    },
    {
        {TypeIndex::Map, "empty"}, optimizeFunctionEmpty<true>,
    },
    {
        {TypeIndex::Map, "notEmpty"}, optimizeFunctionEmpty<false>,
    },
    {
        {TypeIndex::Map, "mapKeys"},
        [](QueryTreeNodePtr & node, FunctionNode &, ColumnContext & ctx)
        {
            /// Replace `mapKeys(map_argument)` with `map_argument.keys`
            const auto & data_type_map = assert_cast<const DataTypeMap &>(*ctx.column.type);
            auto key_type = std::make_shared<DataTypeArray>(data_type_map.getKeyType());

            NameAndTypePair column{ctx.column.name + ".keys", key_type};
            if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
                return;
            node = std::make_shared<ColumnNode>(column, ctx.column_source);
        },
    },
    {
        {TypeIndex::Map, "mapValues"},
        [](QueryTreeNodePtr & node, FunctionNode &, ColumnContext & ctx)
        {
            /// Replace `mapValues(map_argument)` with `map_argument.values`
            const auto & data_type_map = assert_cast<const DataTypeMap &>(*ctx.column.type);
            auto value_type = std::make_shared<DataTypeArray>(data_type_map.getValueType());

            NameAndTypePair column{ctx.column.name + ".values", value_type};
            if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
                return;
            node = std::make_shared<ColumnNode>(column, ctx.column_source);
        },
    },
    {
        {TypeIndex::Map, "mapContainsKey"},
        [](QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
        {
            /// Replace `mapContainsKey(map_argument, argument)` with `has(map_argument.keys, argument)`
            const auto & data_type_map = assert_cast<const DataTypeMap &>(*ctx.column.type);

            NameAndTypePair column{ctx.column.name + ".keys", std::make_shared<DataTypeArray>(data_type_map.getKeyType())};
            if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
                return;
            auto & function_arguments_nodes = function_node.getArguments().getNodes();

            auto has_function_argument = std::make_shared<ColumnNode>(column, ctx.column_source);
            function_arguments_nodes[0] = std::move(has_function_argument);

            resolveOrdinaryFunctionNodeByName(function_node, "has", ctx.context);
        },
    },
    {
        {TypeIndex::Nullable, "count"},
        [](QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
        {
            /// Replace `count(nullable_argument)` with `sum(not(nullable_argument.null))`
            NameAndTypePair column{ctx.column.name + ".null", std::make_shared<DataTypeUInt8>()};
            if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
                return;
            auto & function_arguments_nodes = function_node.getArguments().getNodes();

            auto new_column_node = std::make_shared<ColumnNode>(column, ctx.column_source);
            auto function_node_not = std::make_shared<FunctionNode>("not");
            function_node_not->markAsOperator();

            function_node_not->getArguments().getNodes().push_back(std::move(new_column_node));
            resolveOrdinaryFunctionNodeByName(*function_node_not, "not", ctx.context);

            function_arguments_nodes = {std::move(function_node_not)};
            resolveAggregateFunctionNodeByName(function_node, "sum");
        },
    },
    {
        {TypeIndex::Nullable, "isNull"},
        [](QueryTreeNodePtr & node, FunctionNode &, ColumnContext & ctx)
        {
            /// Replace `isNull(nullable_argument)` with `nullable_argument.null`
            NameAndTypePair column{ctx.column.name + ".null", std::make_shared<DataTypeUInt8>()};
            if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
                return;

            /// For nested Nullable types (e.g. Nullable(Tuple(... Nullable(T) ...))),
            /// the .null subcolumn in storage is Nullable(UInt8), not UInt8.
            /// Using it with a hardcoded UInt8 type causes a type mismatch at runtime.
            if (auto * table_node = ctx.column_source->as<TableNode>())
            {
                auto actual = table_node->getStorageSnapshot()->tryGetColumn(
                    GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), column.name);
                if (actual && actual->type->isNullable())
                    return;
            }

            node = std::make_shared<ColumnNode>(column, ctx.column_source);
        },
    },
    {
        {TypeIndex::Nullable, "isNotNull"},
        [](QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
        {
            /// Replace `isNotNull(nullable_argument)` with `not(nullable_argument.null)`
            NameAndTypePair column{ctx.column.name + ".null", std::make_shared<DataTypeUInt8>()};
            if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name))
                return;

            /// Same guard as isNull above: nested Nullable .null subcolumn may itself be Nullable.
            if (auto * table_node = ctx.column_source->as<TableNode>())
            {
                auto actual = table_node->getStorageSnapshot()->tryGetColumn(
                    GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), column.name);
                if (actual && actual->type->isNullable())
                    return;
            }

            auto & function_arguments_nodes = function_node.getArguments().getNodes();

            function_arguments_nodes = {std::make_shared<ColumnNode>(column, ctx.column_source)};
            resolveOrdinaryFunctionNodeByName(function_node, "not", ctx.context);
        },
    },
    {
        {TypeIndex::Tuple, "tupleElement"}, optimizeTupleOrVariantElement<DataTypeTuple>,
    },
    {
        {TypeIndex::Variant, "variantElement"}, optimizeTupleOrVariantElement<DataTypeVariant>,
    },
    {
        {TypeIndex::QBit, "tupleElement"}, optimizeTupleOrVariantElement<DataTypeQBit>, /// QBit uses tupleElement for subcolumns
    },
    {
        {TypeIndex::Object, "distinctJSONPaths"}, optimizeDistinctJSONPaths,
    },
};

bool canOptimizeWithWherePrewhereOrGroupBy(const String & function_name)
{
    /// Optimization for distinctJSONPaths works correctly only if we request distinct JSON paths across whole table.
    return function_name != "distinctJSONPaths";
}

std::tuple<FunctionNode *, ColumnNode *, TableNode *> getTypedNodesForOptimization(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    auto * function_node = node->as<FunctionNode>();
    if (!function_node)
        return {};

    auto & function_arguments_nodes = function_node->getArguments().getNodes();
    if (function_arguments_nodes.empty() || function_arguments_nodes.size() > 2)
        return {};

    auto * first_argument_column_node = function_arguments_nodes.front()->as<ColumnNode>();
    if (!first_argument_column_node || first_argument_column_node->getColumnName() == "__grouping_set")
        return {};

    auto column_source = first_argument_column_node->getColumnSource();
    auto * table_node = column_source->as<TableNode>();
    if (!table_node)
        return {};

    const auto & storage = table_node->getStorage();
    const auto & storage_snapshot = table_node->getStorageSnapshot();
    auto column = first_argument_column_node->getColumn();

    /// If view source is set we cannot optimize because it doesn't support moving functions to subcolumns.
    /// The storage is replaced to the view source but it happens only after building a query tree and applying passes.
    auto view_source = context->getViewSource();
    if (view_source && view_source->getStorageID().getFullNameNotQuoted() == storage->getStorageID().getFullNameNotQuoted())
        return {};

    if (!storage->supportsOptimizationToSubcolumns() || storage->isVirtualColumn(column.name, storage_snapshot->metadata))
        return {};

    auto column_in_table = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), column.name);
    if (!column_in_table || !column_in_table->type->equals(*column.type))
        return {};

    return std::make_tuple(function_node, first_argument_column_node, table_node);
}

/// First pass collects info about identifiers to determine which identifiers are allowed to optimize.
class FunctionToSubcolumnsVisitorFirstPass : public InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorFirstPass>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorFirstPass>;
    using Base::Base;

    void enterImpl(const QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_functions_to_subcolumns])
            return;

        if (auto * table_node = node->as<TableNode>())
        {
            enterImpl(*table_node);
            return;
        }

        if (auto * column_node = node->as<ColumnNode>())
        {
            enterImpl(*column_node);
            return;
        }

        auto [function_node, first_argument_node, table_node] = getTypedNodesForOptimization(node, getContext());
        if (function_node && first_argument_node && table_node)
        {
            enterImpl(*function_node, *first_argument_node, *table_node);
            return;
        }

        if (const auto * /*join_node*/ _ = node->as<JoinNode>())
        {
            can_wrap_result_columns_with_nullable |= getContext()->getSettingsRef()[Setting::join_use_nulls];
            return;
        }

        if (const auto * /*cross_join_node*/ _ = node->as<CrossJoinNode>())
        {
            can_wrap_result_columns_with_nullable |= getContext()->getSettingsRef()[Setting::join_use_nulls];
            return;
        }

        if (const auto * query_node = node->as<QueryNode>())
        {
            if (query_node->isGroupByWithCube() || query_node->isGroupByWithRollup() || query_node->isGroupByWithGroupingSets())
                can_wrap_result_columns_with_nullable |= getContext()->getSettingsRef()[Setting::group_by_use_nulls];
            has_where_prewhere_or_group_by = query_node->hasWhere() || query_node->hasPrewhere() || query_node->hasGroupBy();
            return;
        }
    }

    std::unordered_set<Identifier> getIdentifiersToOptimize() const
    {
        if (can_wrap_result_columns_with_nullable)
        {
            /// Do not optimize if we have JOIN with setting join_use_null.
            /// Do not optimize if we have GROUP BY WITH ROLLUP/CUBE/GROUPING SETS with setting group_by_use_nulls.
            /// It may change the behaviour if subcolumn can be converted
            /// to Nullable while the original column cannot (e.g. for Array type).
            return {};
        }

        /// TODO(ab): need to optimize for prewhere anyway
        /// Do not optimize if full column is requested in other context.
        /// It doesn't make sense because it doesn't reduce amount of read data
        /// and optimized functions are not computation heavy. But introducing
        /// new identifier complicates query analysis and may break it.
        ///
        /// E.g. query:
        ///     SELECT n FROM table GROUP BY n HAVING isNotNull(n)
        /// may be optimized to incorrect query:
        ///     SELECT n FROM table GROUP BY n HAVING not(n.null)
        /// Will produce: `n.null` is not under aggregate function and not in GROUP BY keys)
        ///
        /// Do not optimize index columns (primary, min-max, secondary),
        /// because otherwise analysis of indexes may be broken.
        /// TODO: handle subcolumns in index analysis.

        std::unordered_set<Identifier> identifiers_to_optimize;
        for (const auto & [identifier, count] : optimized_identifiers_count)
        {
            if (all_key_columns.contains(identifier))
                continue;

            auto it = identifiers_count.find(identifier);
            if (it != identifiers_count.end() && it->second == count)
                identifiers_to_optimize.insert(identifier);
        }

        return identifiers_to_optimize;
    }

private:
    std::unordered_set<Identifier> all_key_columns;
    std::unordered_map<Identifier, UInt64> identifiers_count;
    std::unordered_map<Identifier, UInt64> optimized_identifiers_count;

    NameSet processed_tables;
    bool can_wrap_result_columns_with_nullable = false;
    bool has_where_prewhere_or_group_by = false;

    void enterImpl(const TableNode & table_node)
    {
        auto table_name = table_node.getStorage()->getStorageID().getFullTableName();

        /// If table occurs in query several times (e.g., in subquery), process only once
        /// because we collect only static properties of the table, which are the same for each occurrence.
        if (!processed_tables.emplace(table_name).second)
            return;

        auto add_key_columns = [&](const auto & key_columns)
        {
            for (const auto & column_name : key_columns)
            {
                Identifier identifier({table_name, column_name});
                all_key_columns.insert(identifier);
            }
        };

        const auto & metadata_snapshot = table_node.getStorageSnapshot()->metadata;
        const auto & primary_key_columns = metadata_snapshot->getColumnsRequiredForPrimaryKey();
        const auto & partition_key_columns = metadata_snapshot->getColumnsRequiredForPartitionKey();

        add_key_columns(primary_key_columns);
        add_key_columns(partition_key_columns);

        for (const auto & index : metadata_snapshot->getSecondaryIndices())
        {
            const auto & index_columns = index.expression->getRequiredColumns();
            add_key_columns(index_columns);
        }
    }

    void enterImpl(const ColumnNode & column_node)
    {
        if (column_node.getColumnName() == "__grouping_set")
            return;

        auto column_source = column_node.getColumnSource();
        auto * table_node = column_source->as<TableNode>();
        if (!table_node)
            return;

        auto table_name = table_node->getStorage()->getStorageID().getFullTableName();
        Identifier qualified_name({table_name, column_node.getColumnName()});

        ++identifiers_count[qualified_name];
    }

    void enterImpl(const FunctionNode & function_node, const ColumnNode & first_argument_column_node, const TableNode & table_node)
    {
        /// For queries with FINAL converting function to subcolumn may alter
        /// special merging algorithms and produce wrong result of query.
        if (table_node.hasTableExpressionModifiers() && table_node.getTableExpressionModifiers()->hasFinal())
            return;

        const auto & column = first_argument_column_node.getColumn();
        auto table_name = table_node.getStorage()->getStorageID().getFullTableName();
        Identifier qualified_name({table_name, column.name});

        if (has_where_prewhere_or_group_by && !canOptimizeWithWherePrewhereOrGroupBy(function_node.getFunctionName()))
            return;

        if (node_transformers.contains({column.type->getTypeId(), function_node.getFunctionName()}))
            ++optimized_identifiers_count[qualified_name];
    }
};

/// Second pass optimizes functions to subcolumns for allowed identifiers.
class FunctionToSubcolumnsVisitorSecondPass : public InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorSecondPass>
{
private:
    std::unordered_set<Identifier> identifiers_to_optimize;
    std::unordered_set<const TableNode *> outer_joined_tables;

public:
    using Base = InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorSecondPass>;
    using Base::Base;

    FunctionToSubcolumnsVisitorSecondPass(ContextPtr context_,
        std::unordered_set<Identifier> identifiers_to_optimize_,
        std::unordered_set<const TableNode *> outer_joined_tables_)
        : Base(std::move(context_))
        , identifiers_to_optimize(std::move(identifiers_to_optimize_))
        , outer_joined_tables(std::move(outer_joined_tables_))
    {
    }

    void enterImpl(QueryTreeNodePtr & node) const
    {
        if (!getSettings()[Setting::optimize_functions_to_subcolumns])
            return;

        auto [function_node, first_argument_column_node, table_node] = getTypedNodesForOptimization(node, getContext());
        if (!function_node || !first_argument_column_node || !table_node)
            return;

        auto column = first_argument_column_node->getColumn();
        auto table_name = table_node->getStorage()->getStorageID().getFullTableName();

        Identifier qualified_name({table_name, column.name});
        if (!identifiers_to_optimize.contains(qualified_name))
            return;

        auto result_type = function_node->getResultType();
        auto transformer_it = node_transformers.find({column.type->getTypeId(), function_node->getFunctionName()});

        if (transformer_it != node_transformers.end() && (transformer_it->first.first != TypeIndex::Nullable || !outer_joined_tables.contains(table_node)))
        {
            ColumnContext ctx{std::move(column), first_argument_column_node->getColumnSource(), getContext()};
            transformer_it->second(node, *function_node, ctx);

            if (!result_type->equals(*node->getResultType()))
                node = buildCastFunction(node, result_type, getContext());
        }
    }
};

class GetOuterJoinedTablesVisitor : public InDepthQueryTreeVisitorWithContext<GetOuterJoinedTablesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<GetOuterJoinedTablesVisitor>;
    using Base::Base;

    void enterImpl(const QueryTreeNodePtr & node)
    {
        /// If we are inside the subtree of a JOIN.
        if (!join_nodes_stack.empty())
        {
            const auto * current_join_node = join_nodes_stack.top();

            /// If we are in the left (right) subtree of a LEFT (RIGHT) JOIN, skip this subtree
            /// and mark all tables as outer-joined tables.
            if (isLeftOrFull(current_join_node->getKind()) && current_join_node->getRightTableExpression().get() == node.get())
                need_skip_subtree = true;
            if (isRightOrFull(current_join_node->getKind()) && current_join_node->getLeftTableExpression().get() == node.get())
                need_skip_subtree = true;
        }

        /// Once a JOIN node is entered, keep it on the stack until it is left.
        if (const auto * join_node = node->as<JoinNode>(); join_node && !need_skip_subtree)
        {
            join_nodes_stack.push(join_node);
            return;
        }

        if (const auto * table_node = node->as<TableNode>())
        {
            if (need_skip_subtree)
                outer_joined_tables.insert(table_node);
            return;
        }
    }

    void leaveImpl(const QueryTreeNodePtr & node)
    {
        if (join_nodes_stack.empty())
            return;

        const auto * current_join_node = join_nodes_stack.top();

        /// Leaving the left (or right) subtree of a LEFT (or RIGHT) JOIN.
        if (node.get() == current_join_node->getRightTableExpression().get()
         || node.get() == current_join_node->getLeftTableExpression().get())
            need_skip_subtree = false;

        /// Leaving a JOIN node.
        if (node.get() == current_join_node)
            join_nodes_stack.pop();
    }

    bool need_skip_subtree = false;
    std::stack<const JoinNode *> join_nodes_stack;
    std::unordered_set<const TableNode *> outer_joined_tables;
};

}

void FunctionToSubcolumnsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    FunctionToSubcolumnsVisitorFirstPass first_visitor(context);
    first_visitor.visit(query_tree_node);
    auto identifiers_to_optimize = first_visitor.getIdentifiersToOptimize();

    if (identifiers_to_optimize.empty())
        return;

    /// Tables appearing in LEFT or RIGHT JOIN may produce default values for missing rows.
    /// Inserting a default into a null-mask (of type UInt8) gives a different result than inserting NULL into a Nullable column.
    /// Therefore, functions on Nullable columns from outer-joined tables cannot be optimized.
    GetOuterJoinedTablesVisitor outer_join_visitor(context);
    outer_join_visitor.visit(query_tree_node);
    FunctionToSubcolumnsVisitorSecondPass second_visitor(std::move(context), std::move(identifiers_to_optimize), std::move(outer_join_visitor.outer_joined_tables));
    second_visitor.visit(query_tree_node);
}

}
