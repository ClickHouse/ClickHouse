#include <optional>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/InverseDictionaryLookupPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>

#include <Interpreters/Context.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>
#include <Storages/StorageDictionary.h>
#include <TableFunctions/ITableFunction.h>

#include <Access/ContextAccess.h>
#include <Access/Common/AccessType.h>
#include <Core/Settings.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace Setting
{
extern const SettingsUInt64 max_bytes_in_set;
extern const SettingsUInt64 max_rows_in_set;
extern const SettingsOverflowMode set_overflow_mode;
extern const SettingsBool optimize_inverse_dictionary_lookup;
extern const SettingsBool rewrite_in_to_join;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

struct DictGetFunctionInfo
{
    ConstantNodePtr dict_name_node;
    ConstantNodePtr attr_col_name_node;
    QueryTreeNodePtr key_expr_node;

    /// Necessary for type casting for functions like `dictGetString`, `dictGetInt32`, etc
    DataTypePtr return_type;
};

bool isConstantString(const ConstantNodePtr & node)
{
    return node && node->getValue().getType() == Field::Types::String;
}


bool isSupportedDictGetFunction(const String & name)
{
    static const std::unordered_set<String> supported_functions
        = {"dictGet",
           "dictGetString",
           "dictGetInt8",
           "dictGetInt16",
           "dictGetInt32",
           "dictGetInt64",
           "dictGetUInt8",
           "dictGetUInt16",
           "dictGetUInt32",
           "dictGetUInt64",
           "dictGetFloat32",
           "dictGetFloat64",
           "dictGetDate",
           "dictGetDateTime",
           "dictGetUUID",
           "dictGetIPv4",
           "dictGetIPv6"};

    return supported_functions.contains(name);
}

std::optional<DictGetFunctionInfo> tryParseDictFunctionCall(const QueryTreeNodePtr & node)
{
    const auto * function_node = node->as<FunctionNode>();

    if (!function_node || !isSupportedDictGetFunction(function_node->getFunctionName()))
        return std::nullopt;

    const auto & arguments = function_node->getArguments().getNodes();

    if (arguments.size() != 3)
        return std::nullopt;

    DictGetFunctionInfo func_info;
    func_info.dict_name_node = typeid_cast<ConstantNodePtr>(arguments[0]);
    func_info.attr_col_name_node = typeid_cast<ConstantNodePtr>(arguments[1]);

    if (!func_info.dict_name_node || !func_info.attr_col_name_node)
        return std::nullopt;

    /// Check if both constants hold String values
    if (!isConstantString(func_info.dict_name_node) || !isConstantString(func_info.attr_col_name_node))
        return std::nullopt;

    func_info.key_expr_node = arguments[2];
    func_info.return_type = function_node->getResultType();

    return func_info;
}

bool isInMemoryLayout(const String & type_name)
{
    static const std::unordered_set<String> supported_layouts = {
        "Flat",
        "Hashed",
        "HashedArray",
        "SparseHashed",
        "ComplexKeyHashed",
        "ComplexHashedArray",
        "ComplexKeySparseHashed",
    };
    return supported_layouts.contains(type_name);
}

bool hasNullableComponentInComplexKey(const QueryTreeNodePtr & key_expr_node)
{
    auto type = removeNullable(key_expr_node->getResultType());
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
    if (!tuple_type)
        return false;

    for (const auto & element : tuple_type->getElements())
    {
        if (isNullableOrLowCardinalityNullable(element))
            return true;
    }
    return false;
}

bool isRewriteSemanticallySafe(
    const DataTypePtr & dict_attr_type,
    const DataTypePtr & dictget_result_type,
    const Field & attr_null_value,
    const ConstantNode & const_arg_node,
    bool default_is_lhs,
    const String & attr_comparison_function_name,
    const ContextPtr & context)
{
    /// Same underlying type after stripping `Nullable` / `LowCardinality` needed. If attribute `n`
    /// is `UInt32`, `dictGetUInt16(..., 'n', id) = 42` throws because the underlying types differ (`UInt32` vs `UInt16`)
    const bool stripped_types_match
        = removeLowCardinalityAndNullable(dict_attr_type)->equals(*removeLowCardinalityAndNullable(dictget_result_type));
    if (!stripped_types_match)
        return false;

    /// `dictGet` and `IN` don't have the same stored-NULL attribute semantics.
    /// Example: if dictionary has `id = 1, name = NULL`, `dictGet(..., 1) = 'x'` gives
    /// `NULL`. The `IN` rewrite uses `WHERE name = 'x'`, so the row is filtered out and
    /// `1 IN (...)` gives `0`. This is visible in projection or `isNull(predicate)`.
    /// Skip optimization when the attribute can contain `NULL`, including
    /// `LowCardinality(Nullable(...))`.
    if (isNullableOrLowCardinalityNullable(dict_attr_type))
        return false;

    const DataTypePtr const_arg_type = const_arg_node.getResultType();
    const Field & const_arg_value = const_arg_node.getValue();

    auto default_column = ColumnWithTypeAndName(dict_attr_type->createColumnConst(1, attr_null_value), dict_attr_type, "default_value");
    auto const_arg_column = ColumnWithTypeAndName(const_arg_type->createColumnConst(1, const_arg_value), const_arg_type, "const_value");

    ColumnsWithTypeAndName comparison_arguments;
    if (default_is_lhs)
        comparison_arguments = {std::move(default_column), std::move(const_arg_column)};
    else
        comparison_arguments = {std::move(const_arg_column), std::move(default_column)};

    /// `dictGet` and `IN` don't have the same missing-key default semantics.
    /// e.g: `dictGet(..., id) = ''` vs `id IN (SELECT id FROM dictionary(...) WHERE name = '')`
    /// Example: if dictionary has one row `id = 1, name = 'x'`, data has `id = 2`, and
    /// attribute `DEFAULT` is `''`, `dictGet(..., 2)` returns `''`, so
    /// `dictGet(..., id) = ''` is true for `id = 2`. The `IN` rewrite scans only
    /// dictionary keys, so the subquery has no `id = 2` and `2 IN (...)` is false.
    ///
    /// One of the alternatives is to add `OR id NOT IN (SELECT id FROM dictionary(...))` when the
    /// predicate is true for `DEFAULT`, but it requires another set with all dictionary keys.
    /// This can be expensive to materialize, so skip optimization for such case.
    ///
    /// As a result, given the current rewrite, if `const <op> DEFAULT` is false, only then the
    /// transformation is semantically correct.
    Field comparison_result;
    try
    {
        auto function_resolver = FunctionFactory::instance().get(attr_comparison_function_name, context);
        auto comparison_function_base = function_resolver->build(comparison_arguments);
        auto comparison_result_column
            = comparison_function_base->execute(comparison_arguments, comparison_function_base->getResultType(), 1, /* dry_run = */ false);
        comparison_result = (*comparison_result_column)[0];
    }
    catch (const Exception &)
    {
        /// The constant fold runs during optimization and can throw for values that runtime
        /// would not evaluate. Example: `match('', '(')` throws `CANNOT_COMPILE_REGEXP`, but
        /// `id < 0 AND match(dictGetString(...), '(')` can skip the `match` branch due to
        /// short-circuit evaluation. If we throw here, the optimization breaks a query that
        /// works without it. Skip optimization for such case.
        return false;
    }

    if (comparison_result.isNull())
        return false;

    /// Check `const <op> DEFAULT` is false
    UInt64 comparison_result_uint = 0;
    return comparison_result.tryGet<UInt64>(comparison_result_uint) && comparison_result_uint == 0;
}

class InverseDictionaryLookupVisitor : public InDepthQueryTreeVisitorWithContext<InverseDictionaryLookupVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<InverseDictionaryLookupVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_inverse_dictionary_lookup])
            return;

        auto * node_function = node->as<FunctionNode>();

        if (!node_function)
            return;

        static std::unordered_set<String> allowed_comparison_functions = {
            "equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals", "like", "notLike", "ilike", "notILike", "match"};

        const String attr_comparison_function_name = node_function->getFunctionName();
        if (!allowed_comparison_functions.contains(attr_comparison_function_name))
            return;

        auto & arguments = node_function->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        enum class Side
        {
            LHS,
            RHS,
            NONE
        };

        Side dict_side = Side::NONE;
        DictGetFunctionInfo dictget_function_info;

        if (auto info_lhs = tryParseDictFunctionCall(arguments[0]); info_lhs && arguments[1]->as<ConstantNode>())
        {
            dict_side = Side::LHS;
            dictget_function_info = std::move(*info_lhs);
        }
        else if (auto info_rhs = tryParseDictFunctionCall(arguments[1]); info_rhs && arguments[0]->as<ConstantNode>())
        {
            dict_side = Side::RHS;
            dictget_function_info = std::move(*info_rhs);
        }
        else
        {
            return;
        }

        /// Type of the attribute and key columns are not present in the query. So, we have to fetch dictionary and get the column types.
        auto helper = FunctionDictHelper(getContext());
        const String dict_name = dictget_function_info.dict_name_node->getValue().safeGet<String>();
        const auto dict = helper.getDictionary(dict_name);
        if (!dict)
            return;

        const String dict_type_name = dict->getTypeName();

        if (!isInMemoryLayout(dict_type_name))
            return;


        std::vector<NameAndTypePair> key_cols;

        const auto & dict_structure = dict->getStructure();

        if (dict_structure.id)
        {
            /// `dict_structure.id->type` is the declared key type, but for simple-key dictionaries the effective lookup key type
            /// (and `dictionary()` table function schema) is always UInt64. Use `getKeyTypes().front()` to match that.
            ///
            /// This is important for distributed queries: we derive distributed set names from QueryTree hashes (see `PlannerContext::createSetKey()`),
            /// and QueryTree hashing includes resolved types (e.g. `ColumnNode::updateTreeHashImpl()` hashes the column type).
            /// For simple-key dictionaries, `dict_structure.id->type` is the declared type (can be Int64), but shards can re-resolve the key
            /// as UInt64. If we use the declared type here, initiator/shards can hash
            /// different trees -> different `__set_<hash>` names -> remote header mismatch ("Cannot find column ... __set_...").
            chassert(dict_structure.getKeyTypes().size() == 1);
            key_cols.emplace_back(dict_structure.id->name, dict_structure.getKeyTypes().front());
        }
        else if (dict_structure.key) /// composite key
        {
            key_cols.reserve(dict_structure.key->size());
            for (const auto & id : *dict_structure.key)
            {
                key_cols.emplace_back(id.name, id.type);
            }
        }
        else
        {
            return;
        }

        /// For complex-key dictionaries, `dictGet` and `IN` don't have the same `NULL` key semantics.
        /// e.g: `dictGet(..., (k1, k2))` vs `(k1, k2) IN (SELECT k1, k2 FROM dictionary(...))`
        /// Example: if `k1` is `Nullable(UInt64)` and the dictionary has `(NULL, 'a')`,
        /// `dictGet(..., (k1, k2))` can match it, but `(NULL, 'a') IN (...)` is not a match
        /// with `transform_null_in = 0`.
        /// Single-key dictionaries are not affected. Example: if `id` is `Nullable(UInt64)`,
        /// `dictGet(..., id) = 'x'` gives `NULL` for `id = NULL`, and `id IN (...)` also gives
        /// `NULL` for `id = NULL`.
        if (dict_structure.key && hasNullableComponentInComplexKey(dictget_function_info.key_expr_node))
            return;

        const String attr_col_name = dictget_function_info.attr_col_name_node->getValue().safeGet<String>();

        if (!dict_structure.hasAttribute(attr_col_name))
            return;

        const DictionaryAttribute & attr = dict_structure.getAttribute(attr_col_name);
        DataTypePtr dict_attr_col_type = attr.type;

        const auto * const_arg_node = (dict_side == Side::LHS) ? arguments[1]->as<ConstantNode>() : arguments[0]->as<ConstantNode>();

        /// Skip rewrites that would change query behavior. Details are in the function.
        if (!isRewriteSemanticallySafe(
                dict_attr_col_type,
                dictget_function_info.return_type,
                attr.null_value,
                *const_arg_node,
                dict_side == Side::LHS,
                attr_comparison_function_name,
                getContext()))
            return;

        const String dictget_function_name = dict_side == Side::LHS ? static_cast<FunctionNode *>(arguments[0].get())->getFunctionName()
                                                                    : static_cast<FunctionNode *>(arguments[1].get())->getFunctionName();

        /// `dictGetKeys` compares casts constant to the attribute column type.
        /// For `dictGet`-family functions that perform an internal type cast of the attribute
        /// (for example, `dictGetDateTime` over a `Date` attribute), rewriting
        ///   dictGetX(dict, attr, key) = const
        /// to
        ///   key IN dictGetKeys(dict, attr, const)
        /// would drop that cast and change the predicate semantics. Therefore this optimization is only allowed
        /// when the function result type is exactly the dictionary attribute type.
        bool can_replace_with_dictgetkeys = (attr_comparison_function_name == "equals")
            && (dictget_function_name == "dictGet" || dict_attr_col_type->equals(*dictget_function_info.return_type));

        if (can_replace_with_dictgetkeys)
        {
            /// Preserve the original result type of the comparison node.
            /// For example, original "equals(...)" might have result type Nullable(UInt8),
            /// while "IN" might return UInt8.
            DataTypePtr original_result_type = node_function->getResultType();
            auto preserve_result_type = [&](QueryTreeNodePtr replacement_node)
            {
                if (original_result_type && replacement_node && !replacement_node->getResultType()->equals(*original_result_type))
                    return createCastFunction(std::move(replacement_node), original_result_type, getContext());
                return replacement_node;
            };

            /// Build dictGetKeys('dict_name', 'attr_name', value_expr)
            auto dict_get_keys_fn = std::make_shared<FunctionNode>("dictGetKeys");
            auto & dict_get_keys_args = dict_get_keys_fn->getArguments().getNodes();

            dict_get_keys_args.push_back(dictget_function_info.dict_name_node);
            dict_get_keys_args.push_back(dictget_function_info.attr_col_name_node);
            dict_get_keys_args.push_back(dict_side == Side::LHS ? arguments[1] : arguments[0]);

            QueryAnalyzer analyzer(false);
            QueryTreeNodePtr node_function_ptr = dict_get_keys_fn;
            analyzer.resolveConstantExpression(node_function_ptr, nullptr, getContext());

            /// `resolveConstantExpression` intentionally skips folding large constants
            /// (see `column->byteSize() < 1_MiB` in `src/Analyzer/Resolve/resolveFunction.cpp`).
            /// In that case `dictGetKeys` remains a `FunctionNode`; fall back to the IN-subquery
            /// path below instead of throwing.
            if (const auto * keys_constant = node_function_ptr->as<ConstantNode>())
            {
                const Field & keys_field = keys_constant->getValue();

                if (keys_field.getType() != Field::Types::Array)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "dictGetKeys expected to return Array field. Actual type: {}", keys_field.getType());

                const auto & keys_array = keys_field.safeGet<Array>();
                const size_t keys_size = keys_array.size();

                /// No keys -> WHERE 0
                if (keys_size == 0)
                {
                    auto zero_type = std::make_shared<DataTypeUInt8>();
                    auto zero_node = std::make_shared<ConstantNode>(Field(UInt8(0)), zero_type);
                    node = preserve_result_type(zero_node);
                    return;
                }

                DataTypePtr key_expr_type = dictget_function_info.key_expr_node->getResultType();

                /// Single key -> key_expr = <that key>
                if (keys_size == 1)
                {
                    const Field & single_key_field = keys_array.front();

                    auto single_key_const = std::make_shared<ConstantNode>(single_key_field, key_expr_type);

                    auto equals_node = std::make_shared<FunctionNode>("equals");
                    equals_node->markAsOperator();
                    equals_node->getArguments().getNodes() = {dictget_function_info.key_expr_node, single_key_const};
                    resolveOrdinaryFunctionNodeByName(*equals_node, "equals", getContext());

                    node = preserve_result_type(equals_node);
                    return;
                }

                /// Multiple keys -> key_expr IN <constant array-of-keys>
                /// keys_constant->getResultType() is Array(T) or Array(Tuple(...))
                auto keys_const_node = std::make_shared<ConstantNode>(keys_field, keys_constant->getResultType());

                auto in_function_node = std::make_shared<FunctionNode>("in");
                in_function_node->markAsOperator();
                in_function_node->getArguments().getNodes() = {dictget_function_info.key_expr_node, keys_const_node};
                resolveOrdinaryFunctionNodeByName(*in_function_node, "in", getContext());

                node = preserve_result_type(in_function_node);
                return;
            }
        }

        if (getSettings()[Setting::rewrite_in_to_join])
            return;

        /// We build an `IN` set from the dictionary subquery, which respects `max_rows_in_set`,
        /// `max_bytes_in_set` and `set_overflow_mode`. With `set_overflow_mode = 'break'`, the set
        /// can be truncated and not contain all required elements, so the optimization can produce
        /// wrong results. Skip optimization for such case.
        if ((getSettings()[Setting::max_rows_in_set] != 0 || getSettings()[Setting::max_bytes_in_set] != 0)
            && getSettings()[Setting::set_overflow_mode] == OverflowMode::BREAK)
            return;

        /// This rewrite turns `dictGet(...)` predicates into `IN (SELECT ... FROM dictionary(...))`.
        /// The `dictionary()` table function requires `CREATE TEMPORARY TABLE`; if that grant is missing,
        /// skip the optimization to avoid `ACCESS_DENIED`.
        if (!getContext()->getAccess()->isGranted(AccessType::CREATE_TEMPORARY_TABLE))
            return;

        auto dict_table_function = std::make_shared<TableFunctionNode>("dictionary");
        dict_table_function->getArguments().getNodes().push_back(dictget_function_info.dict_name_node);

        auto dict_table_storage = std::make_shared<StorageDictionary>(
            StorageID(ITableFunction::getDatabaseName(), "dictionary"),
            dict_name,
            ColumnsDescription{StorageDictionary::getNamesAndTypes(dict_structure, /*validate_id_type*/ false)},
            String{},
            StorageDictionary::Location::Custom,
            getContext());

        dict_table_function->resolve({}, std::move(dict_table_storage), getContext(), {});

        NameAndTypePair attr_col{attr_col_name, dict_attr_col_type};
        auto attr_col_node = std::make_shared<ColumnNode>(attr_col, dict_table_function);

        auto attr_comparison_function_node = std::static_pointer_cast<FunctionNode>(node_function->clone());
        attr_comparison_function_node->markAsOperator();
        /// The clone copies `parenthesized` from `node_function`, but this is now a fresh comparison
        /// node placed inside a synthetic subquery's WHERE — it should not inherit the original
        /// parens.
        attr_comparison_function_node->setParenthesized(false);

        if (dict_side == Side::LHS)
        {
            attr_comparison_function_node->getArguments().getNodes() = { attr_col_node, arguments[1] };
        }
        else
        {
            attr_comparison_function_node->getArguments().getNodes() = { arguments[0], attr_col_node };
        }
        resolveOrdinaryFunctionNodeByName(*attr_comparison_function_node, attr_comparison_function_name, getContext());

        /// SELECT key_col FROM dictionary(dict_name) WHERE attr_name = const_value
        auto subquery_node = std::make_shared<QueryNode>(Context::createCopy(getContext()));
        subquery_node->setIsSubquery(true);
        subquery_node->getJoinTree() = dict_table_function;
        subquery_node->getWhere() = attr_comparison_function_node;

        for (const auto & key_col_node : key_cols)
        {
            subquery_node->getProjection().getNodes().push_back(std::make_shared<ColumnNode>(key_col_node, dict_table_function));
        }
        subquery_node->resolveProjectionColumns(key_cols);

        auto in_function_node = std::make_shared<FunctionNode>("in");
        in_function_node->markAsOperator();
        QueryTreeNodePtr querytree_subquery_node = subquery_node;
        in_function_node->getArguments().getNodes() = {dictget_function_info.key_expr_node, querytree_subquery_node};
        resolveOrdinaryFunctionNodeByName(*in_function_node, "in", getContext());

        /// Preserve the original result type of the comparison node.
        /// For example, original "equals(...)" might have result type Nullable(UInt8),
        /// while "IN" might return UInt8.
        DataTypePtr original_result_type = node_function->getResultType();

        QueryTreeNodePtr replacement_node = in_function_node;
        if (original_result_type && !in_function_node->getResultType()->equals(*original_result_type))
            replacement_node = createCastFunction(in_function_node, original_result_type, getContext());

        node = std::move(replacement_node);
    }
};

}

void InverseDictionaryLookupPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    InverseDictionaryLookupVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
