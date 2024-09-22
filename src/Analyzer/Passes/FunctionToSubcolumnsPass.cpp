#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>

#include <Storages/IStorage.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/JoinNode.h>

#include <Core/Settings.h>

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

void optimizeFunctionLength(QueryTreeNodePtr & node, FunctionNode &, ColumnContext & ctx)
{
    /// Replace `length(argument)` with `argument.size0`
    /// `argument` may be Array or Map.

    NameAndTypePair column{ctx.column.name + ".size0", std::make_shared<DataTypeUInt64>()};
    node = std::make_shared<ColumnNode>(column, ctx.column_source);
}

template <bool positive>
void optimizeFunctionEmpty(QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
{
    /// Replace `empty(argument)` with `equals(argument.size0, 0)` if positive
    /// Replace `notEmpty(argument)` with `notEquals(argument.size0, 0)` if not positive
    /// `argument` may be Array or Map.

    NameAndTypePair column{ctx.column.name + ".size0", std::make_shared<DataTypeUInt64>()};
    auto & function_arguments_nodes = function_node.getArguments().getNodes();

    function_arguments_nodes.clear();
    function_arguments_nodes.push_back(std::make_shared<ColumnNode>(column, ctx.column_source));
    function_arguments_nodes.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

    const auto * function_name = positive ? "equals" : "notEquals";
    resolveOrdinaryFunctionNodeByName(function_node, function_name, ctx.context);
}

String getSubcolumnNameForElement(const Field & value, const DataTypeTuple & data_type_tuple)
{
    if (value.getType() == Field::Types::String)
        return value.safeGet<const String &>();

    if (value.getType() == Field::Types::UInt64)
        return data_type_tuple.getNameByPosition(value.safeGet<UInt64>());

    return "";
}

String getSubcolumnNameForElement(const Field & value, const DataTypeVariant &)
{
    if (value.getType() == Field::Types::String)
        return value.safeGet<const String &>();

    return "";
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
    auto subcolumn_name = getSubcolumnNameForElement(second_argument_constant_node->getValue(), data_type_concrete);

    if (subcolumn_name.empty())
        return;

    NameAndTypePair column{ctx.column.name + "." + subcolumn_name, function_node.getResultType()};
    node = std::make_shared<ColumnNode>(column, ctx.column_source);
}

std::map<std::pair<TypeIndex, String>, NodeToSubcolumnTransformer> node_transformers =
{
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
        [](QueryTreeNodePtr & node, FunctionNode & function_node, ColumnContext & ctx)
        {
            /// Replace `mapKeys(map_argument)` with `map_argument.keys`
            NameAndTypePair column{ctx.column.name + ".keys", function_node.getResultType()};
            node = std::make_shared<ColumnNode>(column, ctx.column_source);
        },
    },
    {
        {TypeIndex::Map, "mapValues"},
        [](QueryTreeNodePtr & node, FunctionNode & function_node, ColumnContext & ctx)
        {
            /// Replace `mapValues(map_argument)` with `map_argument.values`
            NameAndTypePair column{ctx.column.name + ".values", function_node.getResultType()};
            node = std::make_shared<ColumnNode>(column, ctx.column_source);
        },
    },
    {
        {TypeIndex::Map, "mapContains"},
        [](QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
        {
            /// Replace `mapContains(map_argument, argument)` with `has(map_argument.keys, argument)`
            const auto & data_type_map = assert_cast<const DataTypeMap &>(*ctx.column.type);

            NameAndTypePair column{ctx.column.name + ".keys", std::make_shared<DataTypeArray>(data_type_map.getKeyType())};
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
            auto & function_arguments_nodes = function_node.getArguments().getNodes();

            auto new_column_node = std::make_shared<ColumnNode>(column, ctx.column_source);
            auto function_node_not = std::make_shared<FunctionNode>("not");

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
            node = std::make_shared<ColumnNode>(column, ctx.column_source);
        },
    },
    {
        {TypeIndex::Nullable, "isNotNull"},
        [](QueryTreeNodePtr &, FunctionNode & function_node, ColumnContext & ctx)
        {
            /// Replace `isNotNull(nullable_argument)` with `not(nullable_argument.null)`
            NameAndTypePair column{ctx.column.name + ".null", std::make_shared<DataTypeUInt8>()};
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
};

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

    auto column_in_table = storage_snapshot->tryGetColumn(GetColumnsOptions::All, column.name);
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

        if (const auto * join_node = node->as<JoinNode>())
        {
            can_wrap_result_columns_with_nullable |= getContext()->getSettingsRef()[Setting::join_use_nulls];
            return;
        }

        if (const auto * query_node = node->as<QueryNode>())
        {
            if (query_node->isGroupByWithCube() || query_node->isGroupByWithRollup() || query_node->isGroupByWithGroupingSets())
                can_wrap_result_columns_with_nullable |= getContext()->getSettingsRef()[Setting::group_by_use_nulls];
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

    void enterImpl(const TableNode & table_node)
    {
        auto table_name = table_node.getStorage()->getStorageID().getFullTableName();
        if (processed_tables.emplace(table_name).second)
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

        if (node_transformers.contains({column.type->getTypeId(), function_node.getFunctionName()}))
            ++optimized_identifiers_count[qualified_name];
    }
};

/// Second pass optimizes functions to subcolumns for allowed identifiers.
class FunctionToSubcolumnsVisitorSecondPass : public InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorSecondPass>
{
private:
    std::unordered_set<Identifier> identifiers_to_optimize;

public:
    using Base = InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorSecondPass>;
    using Base::Base;

    FunctionToSubcolumnsVisitorSecondPass(ContextPtr context_, std::unordered_set<Identifier> identifiers_to_optimize_)
        : Base(std::move(context_)), identifiers_to_optimize(std::move(identifiers_to_optimize_))
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

        auto transformer_it = node_transformers.find({column.type->getTypeId(), function_node->getFunctionName()});
        if (transformer_it != node_transformers.end())
        {
            ColumnContext ctx{std::move(column), first_argument_column_node->getColumnSource(), getContext()};
            transformer_it->second(node, *function_node, ctx);
        }
    }
};

}

void FunctionToSubcolumnsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    FunctionToSubcolumnsVisitorFirstPass first_visitor(context);
    first_visitor.visit(query_tree_node);
    auto identifiers_to_optimize = first_visitor.getIdentifiersToOptimize();

    if (identifiers_to_optimize.empty())
        return;

    FunctionToSubcolumnsVisitorSecondPass second_visitor(std::move(context), std::move(identifiers_to_optimize));
    second_visitor.visit(query_tree_node);
}

}
