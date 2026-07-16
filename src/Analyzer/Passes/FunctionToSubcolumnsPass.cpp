#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>
#include <DataTypes/DataTypeString.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/NestedUtils.h>

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
#include <Analyzer/QueryNode.h>
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

struct IdentifiersToOptimize
{
    /// Identifiers where ALL uses are optimizable (count matches).
    /// Rewritten unconditionally in every clause.
    std::unordered_set<Identifier> everywhere;

    /// Identifiers that also have plain column references, but have at least one
    /// transformable use in WHERE/PREWHERE. Rewritten ONLY inside WHERE/PREWHERE.
    std::unordered_set<Identifier> filter_only;

    bool empty() const { return everywhere.empty() && filter_only.empty(); }
};

using NodeToSubcolumnTransformer = std::function<void(QueryTreeNodePtr &, FunctionNode &, ColumnContext &)>;

using ChainedNodeToSubcolumnTransformer = std::function<void(
    QueryTreeNodePtr &, FunctionNode &, ColumnContext &,
    std::vector<FunctionNode *> & intermediate_functions)>;

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
bool canOptimizeToSubcolumn(QueryTreeNodePtr column_source, const String & subcolumn_name, bool is_regular_subcolumn = true)
{
    auto * table_node = column_source->as<TableNode>();
    if (!table_node)
        return {};

    const auto & storage_snapshot = table_node->getStorageSnapshot();
    auto get_options = GetColumnsOptions(GetColumnsOptions::All);
    if (is_regular_subcolumn)
        get_options = get_options.withRegularSubcolumns();
    else
        get_options = get_options.withSubcolumns();
    return storage_snapshot->tryGetColumn(get_options, subcolumn_name).has_value();
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

void optimizeFunctionArrayElementForMap(QueryTreeNodePtr & node, FunctionNode & function_node, ColumnContext & ctx)
{
    /// Replace `m['key']` (which is internally `arrayElement(m, 'key')`) with the subcolumn `m.key_<serialized_key>`.

    auto & function_arguments_nodes = function_node.getArguments().getNodes();
    if (function_arguments_nodes.size() != 2)
        return;

    /// The key must be a compile-time constant — dynamic key lookups cannot be rewritten to a fixed subcolumn.
    const auto * second_argument_constant_node = function_arguments_nodes[1]->as<ConstantNode>();
    if (!second_argument_constant_node)
        return;

    const auto & data_type_map = assert_cast<const DataTypeMap &>(*ctx.column.type);
    const auto & key_type = data_type_map.getKeyType();
    auto tmp_key_column = key_type->createColumn();
    /// Verify that the constant value is compatible with the map's key type.
    if (!tmp_key_column->tryInsert(second_argument_constant_node->getValue()))
        return;

    /// Serialize the key to its text representation to construct the subcolumn name,
    /// e.g. the string key "foo" becomes the subcolumn suffix "key_foo".
    WriteBufferFromOwnString buf;
    key_type->getDefaultSerialization()->serializeText(*tmp_key_column, 0, buf, FormatSettings());
    String subcolumn_name = String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + buf.str();

    /// The resulting subcolumn has the map's value type, e.g. `m.key_foo : V` for `Map(K, V)`.
    NameAndTypePair column{ctx.column.name + "." + subcolumn_name, data_type_map.getValueType()};
    /// Use is_regular_subcolumn=false because key subcolumns are not declared as regular subcolumns
    /// of the table schema — they are dynamic subcolumns.
    if (sourceHasColumn(ctx.column_source, column.name) || !canOptimizeToSubcolumn(ctx.column_source, column.name, false))
        return;

    node = std::make_shared<ColumnNode>(column, ctx.column_source);
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

            /// When the column is inside a Nullable(Tuple(...)), the .null subcolumn/nullmap
            /// in storage is Nullable(UInt8), not UInt8, because the type system wraps all
            /// subcolumns of a Nullable(Tuple(...)) with the outer nullability. Using it with
            /// a hardcoded UInt8 type causes a type mismatch at runtime. Skip the optimization.
            if (auto * table_node = ctx.column_source->as<TableNode>())
            {
                auto actual = table_node->getStorageSnapshot()->tryGetColumn(
                    GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns(), column.name);
                if (actual && actual->type->isNullable())
                    return;
            }

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
    {
        {TypeIndex::Map, "arrayElement"}, optimizeFunctionArrayElementForMap,
    },
};

/// Transformers that can be safely applied even when the column is used in
/// primary key, partition key, or secondary index expressions. The rewritten
/// subcolumn form will be handled by index analysis (or the index simply
/// won't be used, which is safe — just potentially slower).
std::set<std::pair<TypeIndex, String>> transformers_safe_with_indexes =
{
    {TypeIndex::Map, "arrayElement"},
};

/// Transformers that should be applied even when the full column is also read
/// elsewhere in the query (e.g., in SELECT alongside WHERE m['key'] = val).
/// Normally the optimizer skips a column if it's used both in a transformable
/// function and as a plain column reference, because introducing a new
/// subcolumn identifier complicates analysis. But for Map key lookups the
/// transformation is beneficial when the occurrence is in WHERE/PREWHERE: only
/// the relevant bucket is read for the filter, while the full map is still read
/// for matching rows in SELECT. The reads are independent and semantically correct.
/// Note: this exception does NOT apply to HAVING or other clauses where the
/// subcolumn would need to appear in GROUP BY.
std::set<std::pair<TypeIndex, String>> transformers_optimize_in_filter_with_full_column =
{
    {TypeIndex::Map, "arrayElement"},
};

/// Optimizes:
///   tupleElement(... tupleElement(arrayElement(ColumnNode(Dynamic), N), 'f1') ..., 'fK')
/// to:
///   arrayElement(ColumnNode("col.:`Array(JSON)`.f1...fK", type), N)
///
/// intermediate_functions contains the chain from outermost to innermost,
/// e.g. [inner_tupleElement_1, ..., inner_tupleElement_M, arrayElement].
void optimizeJSONArrayElement(
    QueryTreeNodePtr & node, FunctionNode & function_node, ColumnContext & ctx,
    std::vector<FunctionNode *> & intermediate_functions)
{
    if (intermediate_functions.empty())
        return;

    /// The last intermediate must be arrayElement with 2 args (col, index).
    auto * array_element_func = intermediate_functions.back();
    if (array_element_func->getFunctionName() != "arrayElement")
        return;

    auto & array_element_args = array_element_func->getArguments().getNodes();
    if (array_element_args.size() != 2)
        return;

    auto array_index_node = array_element_args[1];

    /// Collect field names from the tupleElement chain (outer to inner).
    std::vector<String> field_names;

    /// The outermost tupleElement is function_node itself.
    {
        auto & args = function_node.getArguments().getNodes();
        if (args.size() != 2)
            return;
        const auto * constant = args[1]->as<ConstantNode>();
        if (!constant || constant->getValue().getType() != Field::Types::String)
            return;
        field_names.push_back(constant->getValue().safeGet<String>());
    }

    /// All intermediates except the last (arrayElement) must be tupleElement with constant string second arg.
    for (size_t i = 0; i + 1 < intermediate_functions.size(); ++i)
    {
        auto * inner_func = intermediate_functions[i];
        if (inner_func->getFunctionName() != "tupleElement")
            return;
        auto & args = inner_func->getArguments().getNodes();
        if (args.size() != 2)
            return;
        const auto * constant = args[1]->as<ConstantNode>();
        if (!constant || constant->getValue().getType() != Field::Types::String)
            return;
        field_names.push_back(constant->getValue().safeGet<String>());
    }

    /// Reverse to get inner-to-outer order (the path under Array(JSON)).
    std::reverse(field_names.begin(), field_names.end());

    /// Verify the Dynamic column is a subcolumn of a JSON column.
    /// Use getAllColumnAndSubcolumnPairs to handle nested cases like Tuple(json JSON).
    auto column_source = ctx.column_source;
    auto * table_node = column_source->as<TableNode>();
    if (!table_node)
        return;

    const auto & storage_snapshot = table_node->getStorageSnapshot();
    bool found_json_ancestor = false;
    auto pairs = Nested::getAllColumnAndSubcolumnPairs(ctx.column.name);
    for (auto it = pairs.rbegin(); it != pairs.rend(); ++it)
    {
        auto prefix_col = storage_snapshot->tryGetColumn(
            GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(),
            String(it->first));
        if (prefix_col && prefix_col->type->getTypeId() == TypeIndex::Object)
        {
            found_json_ancestor = true;
            break;
        }
    }

    if (!found_json_ancestor)
        return;

    /// Build the new subcolumn name: "col.:`Array(JSON)`.f1.f2...fK".
    String new_col_name = ctx.column.name + ".:`Array(JSON)`";
    for (const auto & field : field_names)
        new_col_name += "." + field;

    /// Verify the subcolumn exists in storage.
    auto new_column = storage_snapshot->tryGetColumn(
        GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), new_col_name);
    if (!new_column)
        return;

    /// Remember the original result type before rewriting.
    /// For example, json.a[1].b may have type Dynamic(max_types=8) while
    /// arrayElement(col.:`Array(JSON)`.b, N) yields Dynamic(max_types=0).
    auto original_result_type = function_node.getResultType();

    /// Rewrite: replace the whole tupleElement chain with arrayElement(new_column, N).
    auto new_column_node = std::make_shared<ColumnNode>(*new_column, column_source);

    auto & args = function_node.getArguments().getNodes();
    args = {std::move(new_column_node), std::move(array_index_node)};
    resolveOrdinaryFunctionNodeByName(function_node, "arrayElement", ctx.context);

    /// Cast back to the original type if it changed, e.g. Dynamic(max_types=N) -> Dynamic(max_types=0).
    if (!original_result_type->equals(*function_node.getResultType()))
        node = buildCastFunction(node, original_result_type, ctx.context);
}

std::map<std::pair<TypeIndex, String>, ChainedNodeToSubcolumnTransformer> chained_node_transformers =
{
    {
        {TypeIndex::Dynamic, "tupleElement"}, optimizeJSONArrayElement,
    },
};

bool canOptimizeWithWherePrewhereOrGroupBy(const String & function_name)
{
    /// Optimization for distinctJSONPaths works correctly only if we request distinct JSON paths across whole table.
    return function_name != "distinctJSONPaths";
}

/// Follow a chain of trivial ALIAS columns (an ALIAS column whose body is itself a ColumnNode
/// from the same table) down to the underlying storage column. Used to let the function-to-subcolumn
/// rewrite see through `c ALIAS some_storage_column` (possibly chained) and rewrite as if the query
/// had referenced the storage column directly.
///
/// Returns nullptr if any step is not a same-table ColumnNode. In particular this guards against
/// ColumnNodes whose expression is not really a "rename":
///   * non-trivial ALIAS bodies (function calls, casts), where the value differs from the source column.
///   * ARRAY JOIN columns (source is ArrayJoinNode), where the column is an unrolled element.
///   * JOIN USING columns (source is JoinNode, expression is a ListNode), where the value comes from the join.
///   * subquery columns (source is QueryNode or UnionNode), which are not storage columns.
ColumnNode * resolveTrivialAliasChain(ColumnNode * column_node)
{
    auto initial_source = column_node->getColumnSource();
    if (!initial_source->as<TableNode>())
        return nullptr;

    while (column_node->hasExpression())
    {
        auto * inner = column_node->getExpression()->as<ColumnNode>();
        if (!inner)
            return nullptr;
        /// Every step must come from the same TableNode as the outer column. This rejects
        /// ARRAY JOIN, JOIN USING, and subquery-resolved aliases whose expression happens to be
        /// a ColumnNode of an unrelated source. Substituting those would change query semantics.
        if (inner->getColumnSource().get() != initial_source.get())
            return nullptr;
        column_node = inner;
    }
    return column_node;
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

    /// For ALIAS columns whose body is just another ColumnNode (i.e. `c ALIAS some_storage_column`, maybe chained),
    /// follow the chain to the underlying storage column and rewrite as if the query had referenced it directly.
    if (first_argument_column_node->hasExpression())
    {
        first_argument_column_node = resolveTrivialAliasChain(first_argument_column_node);
        if (!first_argument_column_node)
            return {};
    }

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

    if (!storage->supportsOptimizationToSubcolumns() || storage_snapshot->metadata->isVirtualColumn(column.name))
        return {};

    auto column_in_table = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), column.name);
    if (!column_in_table || !column_in_table->type->equals(*column.type))
        return {};

    return std::make_tuple(function_node, first_argument_column_node, table_node);
}

/// Like getTypedNodesForOptimization, but walks through first-argument
/// function chains to find the underlying ColumnNode.
/// Returns the outermost function, the underlying column, the table,
/// and the chain of intermediate function nodes.
std::tuple<FunctionNode *, ColumnNode *, TableNode *, std::vector<FunctionNode *>>
getTypedNodesForChainedOptimization(const QueryTreeNodePtr & node, const ContextPtr & context)
{
    auto * function_node = node->as<FunctionNode>();
    if (!function_node)
        return {};

    auto & function_arguments_nodes = function_node->getArguments().getNodes();
    if (function_arguments_nodes.empty())
        return {};

    /// Walk through first arguments, collecting intermediate FunctionNodes,
    /// until we find a ColumnNode.
    std::vector<FunctionNode *> intermediates;
    QueryTreeNodePtr current = function_arguments_nodes[0];
    while (auto * inner_func = current->as<FunctionNode>())
    {
        intermediates.push_back(inner_func);
        auto & inner_args = inner_func->getArguments().getNodes();
        if (inner_args.empty())
            return {};
        current = inner_args[0];
    }

    /// Must have at least one intermediate (otherwise getTypedNodesForOptimization handles it).
    if (intermediates.empty())
        return {};

    auto * first_argument_column_node = current->as<ColumnNode>();
    if (!first_argument_column_node
        || first_argument_column_node->getColumnName() == "__grouping_set"
        || first_argument_column_node->hasExpression())
        return {};

    auto column_source = first_argument_column_node->getColumnSource();
    auto * table_node = column_source->as<TableNode>();
    if (!table_node)
        return {};

    const auto & storage = table_node->getStorage();
    const auto & storage_snapshot = table_node->getStorageSnapshot();
    auto column = first_argument_column_node->getColumn();

    /// Same checks as getTypedNodesForOptimization.
    auto view_source = context->getViewSource();
    if (view_source && view_source->getStorageID().getFullNameNotQuoted() == storage->getStorageID().getFullNameNotQuoted())
        return {};

    if (!storage->supportsOptimizationToSubcolumns() || storage_snapshot->metadata->isVirtualColumn(column.name))
        return {};

    auto column_in_table = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), column.name);
    if (!column_in_table || !column_in_table->type->equals(*column.type))
        return {};

    return {function_node, first_argument_column_node, table_node, std::move(intermediates)};
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

        /// Skip nodes that are inner parts of an already-matched chained pattern.
        /// Without this, inner tupleElements in multi-level chains like
        /// tupleElement(tupleElement(arrayElement(col, N), 'b'), 'c')
        /// would also match chained_node_transformers and double-count
        /// optimized_identifiers_count for the underlying column.
        if (chained_pattern_inner_nodes.contains(node.get()))
            return;

        /// Chained match (e.g. tupleElement over Dynamic through arrayElement).
        auto [chain_func, chain_col, chain_table, intermediates] = getTypedNodesForChainedOptimization(node, getContext());
        if (chain_func && chain_col && chain_table)
        {
            enterImpl(*chain_func, *chain_col, *chain_table, intermediates);
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
            /// Push a placeholder for this query level; needChildVisit will update it
            /// to true when we descend into WHERE or PREWHERE.
            in_where_prewhere_stack.push_back(false);
            return;
        }
    }

    void leaveImpl(const QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_functions_to_subcolumns])
            return;

        if (node->as<QueryNode>())
            in_where_prewhere_stack.pop_back();
    }

    bool needChildVisit(const QueryTreeNodePtr & parent, const QueryTreeNodePtr & child)
    {
        if (const auto * query_node = parent->as<QueryNode>())
        {
            if (!in_where_prewhere_stack.empty())
            {
                bool is_where = query_node->hasWhere() && child.get() == query_node->getWhere().get();
                bool is_prewhere = query_node->hasPrewhere() && child.get() == query_node->getPrewhere().get();
                in_where_prewhere_stack.back() = is_where || is_prewhere;
            }
        }
        return true;
    }

    IdentifiersToOptimize getIdentifiersToOptimize() const
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
        /// When all uses of an identifier are optimizable (count matches), the
        /// identifier goes into `everywhere` — it is rewritten in every clause.
        ///
        /// When there are also plain column references but a transformable use
        /// exists in WHERE/PREWHERE (recorded in `identifiers_with_filter_optimization`),
        /// the identifier goes into `filter_only` — it is rewritten only inside
        /// WHERE/PREWHERE by the second pass. This is beneficial for Map key
        /// lookups: only the relevant bucket is read for the filter while the
        /// full Map is still read for matching rows in SELECT.
        ///
        /// Do not optimize index columns (primary, min-max, secondary),
        /// because otherwise analysis of indexes may be broken.
        /// Exception: transformers listed in `transformers_safe_with_indexes` are allowed
        /// even for indexed columns, provided ALL optimizable uses of the column are safe.
        /// TODO: handle all subcolumns in index analysis.

        IdentifiersToOptimize result;
        for (const auto & [identifier, count] : optimized_identifiers_count)
        {
            if (all_key_columns.contains(identifier))
            {
                auto safe_it = optimized_identifiers_index_safe_count.find(identifier);
                if (safe_it == optimized_identifiers_index_safe_count.end() || safe_it->second != count)
                    continue;
            }

            auto it = identifiers_count.find(identifier);
            if (it == identifiers_count.end())
                continue;

            if (it->second == count)
                result.everywhere.insert(identifier);
            else if (identifiers_with_filter_optimization.contains(identifier))
                result.filter_only.insert(identifier);
        }

        return result;
    }

private:
    std::unordered_set<Identifier> all_key_columns;
    std::unordered_map<Identifier, UInt64> identifiers_count;
    std::unordered_map<Identifier, UInt64> optimized_identifiers_count;
    /// Counts only uses of transformers from `transformers_safe_with_indexes`.
    std::unordered_map<Identifier, UInt64> optimized_identifiers_index_safe_count;
    /// Identifiers that have at least one use of a transformer from
    /// `transformers_optimize_in_filter_with_full_column` inside WHERE or PREWHERE.
    /// These are optimized even when the column is also read as a full column elsewhere.
    std::unordered_set<Identifier> identifiers_with_filter_optimization;

    /// Stack tracking whether the current node is inside a WHERE or PREWHERE clause.
    /// One entry per QueryNode depth; true means we are inside WHERE/PREWHERE.
    std::vector<bool> in_where_prewhere_stack;

    NameSet processed_tables;
    bool can_wrap_result_columns_with_nullable = false;
    bool has_where_prewhere_or_group_by = false;

    /// Intermediate function nodes of already-matched chained patterns.
    /// Prevents double-counting in multi-level chains like
    /// tupleElement(tupleElement(arrayElement(col, N), 'b'), 'c').
    std::unordered_set<const IQueryTreeNode *> chained_pattern_inner_nodes;

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

        auto transformer_key = std::make_pair(column.type->getTypeId(), function_node.getFunctionName());
        if (node_transformers.contains(transformer_key))
        {
            ++optimized_identifiers_count[qualified_name];
            if (transformers_safe_with_indexes.contains(transformer_key))
                ++optimized_identifiers_index_safe_count[qualified_name];
            if (transformers_optimize_in_filter_with_full_column.contains(transformer_key)
                && !in_where_prewhere_stack.empty() && in_where_prewhere_stack.back())
                identifiers_with_filter_optimization.insert(qualified_name);
        }
    }

    void enterImpl(
        const FunctionNode & function_node, const ColumnNode & first_argument_column_node,
        const TableNode & table_node, std::vector<FunctionNode *> & intermediates)
    {
        if (table_node.hasTableExpressionModifiers() && table_node.getTableExpressionModifiers()->hasFinal())
            return;

        const auto & column = first_argument_column_node.getColumn();
        auto table_name = table_node.getStorage()->getStorageID().getFullTableName();

        if (has_where_prewhere_or_group_by && !canOptimizeWithWherePrewhereOrGroupBy(function_node.getFunctionName()))
            return;

        if (chained_node_transformers.contains({column.type->getTypeId(), function_node.getFunctionName()}))
        {
            Identifier qualified_name({table_name, column.name});
            ++optimized_identifiers_count[qualified_name];

            /// Mark intermediate nodes to prevent double-counting.
            for (auto * func : intermediates)
                chained_pattern_inner_nodes.insert(func);
        }
    }
};

/// Second pass optimizes functions to subcolumns for allowed identifiers.
/// For identifiers in `filter_only`, the rewrite is restricted to WHERE/PREWHERE
/// clauses only, because in post-aggregation clauses (HAVING, ORDER BY, etc.)
/// the subcolumn would not be present in the block after GROUP BY.
class FunctionToSubcolumnsVisitorSecondPass : public InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorSecondPass>
{
private:
    IdentifiersToOptimize identifiers_to_optimize;
    std::unordered_set<const TableNode *> outer_joined_tables;

    /// Stack tracking whether the current node is inside a WHERE or PREWHERE clause.
    /// One entry per QueryNode depth; true means we are inside WHERE/PREWHERE.
    std::vector<bool> in_where_prewhere_stack;

public:
    using Base = InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitorSecondPass>;
    using Base::Base;

    FunctionToSubcolumnsVisitorSecondPass(ContextPtr context_,
        IdentifiersToOptimize identifiers_to_optimize_,
        std::unordered_set<const TableNode *> outer_joined_tables_)
        : Base(std::move(context_))
        , identifiers_to_optimize(std::move(identifiers_to_optimize_))
        , outer_joined_tables(std::move(outer_joined_tables_))
    {
    }

    bool needChildVisit(const QueryTreeNodePtr & parent, const QueryTreeNodePtr & child)
    {
        if (const auto * query_node = parent->as<QueryNode>())
        {
            if (!in_where_prewhere_stack.empty())
            {
                bool is_where = query_node->hasWhere() && child.get() == query_node->getWhere().get();
                bool is_prewhere = query_node->hasPrewhere() && child.get() == query_node->getPrewhere().get();
                in_where_prewhere_stack.back() = is_where || is_prewhere;
            }
        }
        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_functions_to_subcolumns])
            return;

        if (node->as<QueryNode>())
        {
            in_where_prewhere_stack.push_back(false);
            return;
        }

        /// Direct match: first argument is a ColumnNode.
        /// Restructured from "if (!match) return" to "if (match) { ... } return"
        /// so that failed direct matches fall through to the chained match below.
        auto [function_node, first_argument_column_node, table_node] = getTypedNodesForOptimization(node, getContext());
        if (function_node && first_argument_column_node && table_node)
        {
            auto column = first_argument_column_node->getColumn();
            auto table_name = table_node->getStorage()->getStorageID().getFullTableName();

            Identifier qualified_name({table_name, column.name});

            /// For "filter_only" identifiers, only optimize when inside WHERE/PREWHERE.
            bool should_optimize = identifiers_to_optimize.everywhere.contains(qualified_name);
            if (!should_optimize
                && identifiers_to_optimize.filter_only.contains(qualified_name)
                && !in_where_prewhere_stack.empty()
                && in_where_prewhere_stack.back())
                should_optimize = true;

            if (!should_optimize)
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
            return;
        }

        /// Chained match: first argument is a chain of functions with a ColumnNode at the bottom.
        auto [chain_func, chain_col, chain_table, intermediates] = getTypedNodesForChainedOptimization(node, getContext());
        if (chain_func && chain_col && chain_table)
        {
            auto column = chain_col->getColumn();
            auto table_name = chain_table->getStorage()->getStorageID().getFullTableName();
            Identifier qualified_name({table_name, column.name});

            if (!identifiers_to_optimize.everywhere.contains(qualified_name))
                return;

            auto it = chained_node_transformers.find({column.type->getTypeId(), chain_func->getFunctionName()});
            if (it != chained_node_transformers.end()
                && (it->first.first != TypeIndex::Nullable || !outer_joined_tables.contains(chain_table)))
            {
                auto result_type = chain_func->getResultType();
                ColumnContext ctx{std::move(column), chain_col->getColumnSource(), getContext()};
                it->second(node, *chain_func, ctx, intermediates);

                if (!result_type->equals(*node->getResultType()))
                    node = buildCastFunction(node, result_type, getContext());
            }
        }
    }

    void leaveImpl(const QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_functions_to_subcolumns])
            return;

        if (node->as<QueryNode>())
            in_where_prewhere_stack.pop_back();
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
