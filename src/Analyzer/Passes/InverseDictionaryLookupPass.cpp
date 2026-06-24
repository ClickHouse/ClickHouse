#include <optional>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/InverseDictionaryLookupPass.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>

#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
namespace Setting
{
extern const SettingsUInt64 max_bytes_in_set;
extern const SettingsUInt64 max_rows_in_set;
extern const SettingsOverflowMode set_overflow_mode;
extern const SettingsBool optimize_inverse_dictionary_lookup;
extern const SettingsBool rewrite_in_to_join;
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

enum class FunctionT
{
    invalid_t,
    in_t,
    comparison_t,
};

struct ConstantListCheckResult
{
    bool has_default_value{};
    bool has_null{};
};

ConstantListCheckResult
checkConstantList(const Field & default_value, const ConstantNode * constant_node, const DataTypePtr & dict_attr_type)
{
    ConstantListCheckResult result;
    const auto & constant_field = constant_node->getValue();

    auto check_element = [&](const Field & element)
    {
        if (element.isNull())
        {
            result.has_null = true;
            return;
        }
        Field converted = tryConvertFieldToType(element, *dict_attr_type);
        if (!converted.isNull() && converted == default_value)
            result.has_default_value = true;
    };

    if (constant_field.getType() == Field::Types::Tuple)
    {
        for (const auto & element : constant_field.safeGet<Tuple>())
            check_element(element);
    }
    else if (constant_field.getType() == Field::Types::Array)
    {
        for (const auto & element : constant_field.safeGet<Array>())
            check_element(element);
    }
    else
    {
        check_element(constant_field);
    }

    return result;
}

template <FunctionT T>
std::optional<DictGetFunctionInfo> tryParseDictFunctionCall(const QueryTreeNodePtr & node)
{
    const auto * function_node = node->as<FunctionNode>();

    if (!function_node)
        return std::nullopt;

    const auto & function_name = function_node->getFunctionName();

    if constexpr (T == FunctionT::in_t)
        if (function_node->getResultType()->isNullable())
            return std::nullopt;

    if (!isSupportedDictGetFunction(function_name))
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

template <typename Node>
void resolveNode(const Node & node, const ContextPtr & context)
{
    if (node->isResolved())
        return;

    QueryTreeNodePtr querytree_node = node;
    QueryAnalysisPass(/*only_analyze*/ false).run(querytree_node, context);
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

bool isRewriteSemanticallySafe(const DataTypePtr & dict_attr_type, const DataTypePtr & dictget_result_type)
{
    /// Same underlying type after stripping `Nullable` / `LowCardinality` needed. If attribute `n`
    /// is `UInt32`, `dictGetUInt16(..., 'n', id) = 42` throws because the underlying types differ (`UInt32` vs `UInt16`)
    const bool stripped_types_match
        = removeLowCardinalityAndNullable(dict_attr_type)->equals(*removeLowCardinalityAndNullable(dictget_result_type));
    // if (!stripped_types_match)
    //     return false;

    /// `dictGet` and `IN` don't have the same stored-NULL attribute semantics.
    /// Example: if dictionary has `id = 1, name = NULL`, `dictGet(..., 1) = 'x'` gives
    /// `NULL`. The `IN` rewrite uses `WHERE name = 'x'`, so the row is filtered out and
    /// `1 IN (...)` gives `0`. This is visible in projection or `isNull(predicate)`.
    /// Skip optimization when the attribute can contain `NULL`, including
    /// `LowCardinality(Nullable(...))`.
    const bool is_nullable_or_low_cardinality_nullable = isNullableOrLowCardinalityNullable(dict_attr_type);

    return stripped_types_match && !is_nullable_or_low_cardinality_nullable;

    // if (isNullableOrLowCardinalityNullable(dict_attr_type))
    //     return false;
    //
    // return true;

    // /// `dictGet` and `IN` don't have the same missing-key default semantics.
    // /// e.g: `dictGet(..., id) = ''` vs `id IN (SELECT id FROM dictionary(...) WHERE name = '')`
    // /// Example: if dictionary has one row `id = 1, name = 'x'`, data has `id = 2`, and
    // /// attribute `DEFAULT` is `''`, `dictGet(..., 2)` returns `''`, so
    // /// `dictGet(..., id) = ''` is true for `id = 2`. The `IN` rewrite scans only
    // /// dictionary keys, so the subquery has no `id = 2` and `2 IN (...)` is false.
    // ///
    // /// One of the alternatives is to add `OR id NOT IN (SELECT id FROM dictionary(...))` when the
    // /// predicate is true for `DEFAULT`, but it requires another set with all dictionary keys.
    // /// This can be expensive to materialize, so skip optimization for such case.
    // ///
    // /// As a result, given the current rewrite, if `const <op> DEFAULT` is false, only then the
    // /// transformation is semantically correct.
    // Field comparison_result;
    // try
    // {
    //     auto function_resolver = FunctionFactory::instance().get(attr_comparison_function_name, context);
    //     auto comparison_function_base = function_resolver->build(comparison_arguments);
    //     auto comparison_result_column
    //         = comparison_function_base->execute(comparison_arguments, comparison_function_base->getResultType(), 1, /* dry_run = */ false);
    //     comparison_result = (*comparison_result_column)[0];
    // }
    // catch (const Exception &)
    // {
    //     /// The constant fold runs during optimization and can throw for values that runtime
    //     /// would not evaluate. Example: `match('', '(')` throws `CANNOT_COMPILE_REGEXP`, but
    //     /// `id < 0 AND match(dictGetString(...), '(')` can skip the `match` branch due to
    //     /// short-circuit evaluation. If we throw here, the optimization breaks a query that
    //     /// works without it. Skip optimization for such case.
    //     return false;
    // }

    // if (comparison_result.isNull())
    //     return false;

    // /// Check `const <op> DEFAULT` is false
    // UInt64 comparison_result_uint = 0;
    // return comparison_result.tryGet<UInt64>(comparison_result_uint) && comparison_result_uint == 0;
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

        if (getSettings()[Setting::rewrite_in_to_join])
            return;

        /// We build an `IN` set from the dictionary subquery, which respects `max_rows_in_set`,
        /// `max_bytes_in_set` and `set_overflow_mode`. With `set_overflow_mode = 'break'`, the set
        /// can be truncated and not contain all required elements, so the optimization can produce
        /// wrong results. Skip optimization for such case.
        if ((getSettings()[Setting::max_rows_in_set] != 0 || getSettings()[Setting::max_bytes_in_set] != 0)
            && getSettings()[Setting::set_overflow_mode] == OverflowMode::BREAK)
            return;

        auto * node_function = node->as<FunctionNode>();

        if (!node_function)
            return;

        const String function_name = node_function->getFunctionName();

        static std::unordered_set<String> allowed_in_functions = {"in", "notIn"};
        static std::unordered_set<String> allowed_comparison_functions = {
            "equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals", "like", "notLike", "ilike", "notILike", "match"};

        FunctionT function_t;

        if (allowed_in_functions.contains(function_name))
        {
            function_t = FunctionT::in_t;
        }
        else if (allowed_comparison_functions.contains(function_name))
        {
            function_t = FunctionT::comparison_t;
        }
        else
        {
            return;
        }

        auto ctx = prepareTransformContext(node_function, function_t);
        if (!ctx || ctx->null_in_list)
            return;

        QueryTreeNodePtr final_in_expr;

        switch (function_t)
        {
            case FunctionT::in_t: {
                bool need_negated_condition = (function_name == "in") ? ctx->default_value_in_list : !ctx->default_value_in_list;
                if (need_negated_condition)
                {
                    /// IN & default_value in list:         key NOT IN (SELECT id WHERE attr NOT IN values)
                    /// NOT IN & default_value not in list: key NOT IN (SELECT id WHERE attr IN values)
                    auto negated_where_condition = buildNegatedWhereCondition(*ctx, function_name);
                    auto subquery = buildSubquery(*ctx, negated_where_condition);

                    auto in_expr = buildInExpression(*ctx, subquery);
                    final_in_expr = buildNotExpression(in_expr);
                }
                else
                {
                    /// IN & default_value not in list:     key IN (SELECT id WHERE attr IN values)
                    /// NOT IN & default_value in list:     key IN (SELECT id WHERE attr NOT IN values)
                    auto where_condition = buildWhereCondition(*ctx, node_function, function_name, function_t);
                    auto subquery = buildSubquery(*ctx, where_condition);
                    final_in_expr = buildInExpression(*ctx, subquery);
                }
                break;
            }
            case FunctionT::comparison_t: {
                auto where_condition = buildWhereCondition(*ctx, node_function, function_name, function_t);
                auto subquery = buildSubquery(*ctx, where_condition);
                final_in_expr = buildInExpression(*ctx, subquery);
                break;
            }
            default: {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected function type {} in InverseDictionaryLookupPass",
                    magic_enum::enum_name(function_t));
            }
        }

        DataTypePtr original_result_type = node_function->getResultType();
        QueryTreeNodePtr replacement_node = final_in_expr;
        if (original_result_type && !final_in_expr->getResultType()->equals(*original_result_type))
            replacement_node = createCastFunction(final_in_expr, original_result_type, getContext());

        node = std::move(replacement_node);
    }

private:
    enum class DictSide
    {
        NONE,
        LHS,
        RHS,
    };

    struct TransformContext
    {
        DictGetFunctionInfo dictget_info;
        DictSide side;
        QueryTreeNodePtr constant_arg; ///< The constant argument (RHS for IN, side-dependent for comparison)
        std::vector<NameAndTypePair> key_cols;
        QueryTreeNodePtr dict_table_function;
        QueryTreeNodePtr attr_col_node_casted;
        bool default_value_in_list{};
        bool null_in_list{};
    };

    /// Validates the function arguments and extracts dictionary/attribute information.
    /// Returns nullopt if the pattern doesn't match or dictionary is not suitable.
    std::optional<TransformContext> prepareTransformContext(FunctionNode * node_function, FunctionT function_t)
    {
        auto & arguments = node_function->getArguments().getNodes();
        if (arguments.size() != 2)
            return std::nullopt;

        DictSide side;
        DictGetFunctionInfo dictget_info;
        QueryTreeNodePtr constant_arg;

        switch (function_t)
        {
            case FunctionT::in_t: {
                auto info = tryParseDictFunctionCall<FunctionT::in_t>(arguments[0]);
                if (!info)
                    return std::nullopt;
                if (!arguments[1]->as<ConstantNode>())
                    return std::nullopt;
                side = DictSide::LHS;
                dictget_info = std::move(*info);
                constant_arg = arguments[1];
                break;
            }
            case FunctionT::comparison_t: {
                if (auto info_lhs = tryParseDictFunctionCall<FunctionT::comparison_t>(arguments[0]);
                    info_lhs && arguments[1]->as<ConstantNode>())
                {
                    side = DictSide::LHS;
                    dictget_info = std::move(*info_lhs);
                    constant_arg = arguments[1];
                }
                else if (
                    auto info_rhs = tryParseDictFunctionCall<FunctionT::comparison_t>(arguments[1]);
                    info_rhs && arguments[0]->as<ConstantNode>())
                {
                    side = DictSide::RHS;
                    dictget_info = std::move(*info_rhs);
                    constant_arg = arguments[0];
                }
                else
                {
                    return std::nullopt;
                }
                break;
            }
            default: {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected function type {} in InverseDictionaryLookupPass",
                    magic_enum::enum_name(function_t));
            }
        }

        auto helper = FunctionDictHelper(getContext());
        const String dict_name = dictget_info.dict_name_node->getValue().safeGet<String>();
        const auto dict = helper.getDictionary(dict_name);
        if (!dict)
            return std::nullopt;

        const String dict_type_name = dict->getTypeName();
        if (!isInMemoryLayout(dict_type_name))
            return std::nullopt;


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
            return std::nullopt;
        }

        /// For complex-key dictionaries, `dictGet` and `IN` don't have the same `NULL` key semantics.
        /// e.g: `dictGet(..., (k1, k2))` vs `(k1, k2) IN (SELECT k1, k2 FROM dictionary(...))`
        /// Example: if `k1` is `Nullable(UInt64)` and the dictionary has `(NULL, 'a')`,
        /// `dictGet(..., (k1, k2))` can match it, but `(NULL, 'a') IN (...)` is not a match
        /// with `transform_null_in = 0`.
        /// Single-key dictionaries are not affected. Example: if `id` is `Nullable(UInt64)`,
        /// `dictGet(..., id) = 'x'` gives `NULL` for `id = NULL`, and `id IN (...)` also gives
        /// `NULL` for `id = NULL`.
        if (dict_structure.key && hasNullableComponentInComplexKey(dictget_info.key_expr_node))
            return std::nullopt;

        const String attr_col_name = dictget_info.attr_col_name_node->getValue().safeGet<String>();
        if (!dict_structure.hasAttribute(attr_col_name))
            return std::nullopt;

        const DictionaryAttribute & attr = dict_structure.getAttribute(attr_col_name);
        DataTypePtr dict_attr_col_type = attr.type;

        // const auto * const_arg_node = (side == DictSide::LHS) ? arguments[1]->as<ConstantNode>() : arguments[0]->as<ConstantNode>();

        /// Skip rewrites that would change query behavior. Details are in the function.
        if (!isRewriteSemanticallySafe(dict_attr_col_type, dictget_info.return_type))
            return std::nullopt;
        const auto & dict_attr = dict_structure.getAttribute(attr_col_name);

        Field default_value = dict_attr.null_value;

        /// For IN/notIn operations, check if the default value is in the constant list.
        ConstantListCheckResult check_result{};
        if (function_t == FunctionT::in_t)
        {
            const auto * constant_node = constant_arg->as<ConstantNode>();
            if (constant_node)
                check_result = checkConstantList(default_value, constant_node, dict_attr_col_type);
        }

        auto dict_table_function = std::make_shared<TableFunctionNode>("dictionary");
        dict_table_function->getArguments().getNodes().push_back(dictget_info.dict_name_node);
        resolveNode(dict_table_function, getContext());

        NameAndTypePair attr_col{attr_col_name, dict_attr_col_type};
        auto attr_col_node = std::make_shared<ColumnNode>(attr_col, dict_table_function);

        QueryTreeNodePtr attr_col_node_casted = attr_col_node;
        if (!attr_col_node->getResultType()->equals(*dictget_info.return_type))
        {
            attr_col_node_casted = createCastFunction(attr_col_node, dictget_info.return_type, getContext());
        }

        return TransformContext{
            .dictget_info = std::move(dictget_info),
            .side = side,
            .constant_arg = std::move(constant_arg),
            .key_cols = std::move(key_cols),
            .dict_table_function = std::move(dict_table_function),
            .attr_col_node_casted = std::move(attr_col_node_casted),
            .default_value_in_list = check_result.has_default_value,
            .null_in_list = check_result.has_null,
        };
    }

    /// Builds the WHERE condition for the subquery.
    /// For IN: builds attr_col IN (consts)
    /// For comparison: builds attr_col op const (respecting side for argument order)
    QueryTreeNodePtr
    buildWhereCondition(const TransformContext & ctx, FunctionNode * original_function, const String & function_name, FunctionT function_t)
    {
        auto where_function_node = std::static_pointer_cast<FunctionNode>(original_function->clone());
        where_function_node->markAsOperator();

        if (function_t == FunctionT::in_t)
        {
            where_function_node->getArguments().getNodes() = {ctx.attr_col_node_casted, ctx.constant_arg};
        }
        else
        {
            if (ctx.side == DictSide::LHS)
                where_function_node->getArguments().getNodes() = {ctx.attr_col_node_casted, ctx.constant_arg};
            else
                where_function_node->getArguments().getNodes() = {ctx.constant_arg, ctx.attr_col_node_casted};
        }

        resolveOrdinaryFunctionNodeByName(*where_function_node, function_name, getContext());
        return where_function_node;
    }

    /// Builds the negated WHERE condition for optimized rewrite.
    /// For IN: builds attr_col NOT IN (consts)
    /// For NOT IN: builds attr_col IN (consts)
    QueryTreeNodePtr buildNegatedWhereCondition(const TransformContext & ctx, const String & original_function_name)
    {
        String negated_function_name = (original_function_name == "in") ? "notIn" : "in";

        auto where_function_node = std::make_shared<FunctionNode>(negated_function_name);
        where_function_node->markAsOperator();
        where_function_node->getArguments().getNodes() = {ctx.attr_col_node_casted, ctx.constant_arg};

        resolveOrdinaryFunctionNodeByName(*where_function_node, negated_function_name, getContext());
        return where_function_node;
    }

    QueryTreeNodePtr buildSubquery(const TransformContext & ctx, const QueryTreeNodePtr & where_condition)
    {
        auto subquery_node = std::make_shared<QueryNode>(Context::createCopy(getContext()));
        subquery_node->setIsSubquery(true);
        subquery_node->getJoinTree() = ctx.dict_table_function;
        subquery_node->getWhere() = where_condition;

        for (const auto & key_col : ctx.key_cols)
        {
            subquery_node->getProjection().getNodes().push_back(std::make_shared<ColumnNode>(key_col, ctx.dict_table_function));
        }
        subquery_node->resolveProjectionColumns(ctx.key_cols);

        return subquery_node;
    }

    QueryTreeNodePtr buildInExpression(const TransformContext & ctx, const QueryTreeNodePtr & subquery)
    {
        auto in_function_node = std::make_shared<FunctionNode>("in");
        in_function_node->markAsOperator();
        in_function_node->getArguments().getNodes() = {ctx.dictget_info.key_expr_node, subquery};
        resolveOrdinaryFunctionNodeByName(*in_function_node, "in", getContext());
        return in_function_node;
    }

    QueryTreeNodePtr buildNotExpression(const QueryTreeNodePtr & expr)
    {
        auto not_function_node = std::make_shared<FunctionNode>("not");
        not_function_node->getArguments().getNodes() = {expr};
        resolveOrdinaryFunctionNodeByName(*not_function_node, "not", getContext());
        return not_function_node;
    }
};

}

void InverseDictionaryLookupPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    /// This rewrite turns `dictGet(...)` predicates into `IN (SELECT ... FROM dictionary(...))`.
    /// The `dictionary()` table function requires `CREATE TEMPORARY TABLE`; if that grant is missing,
    /// skip the optimization to avoid `ACCESS_DENIED`.
    if (!context->getAccess()->isGranted(AccessType::CREATE_TEMPORARY_TABLE))
        return;

    InverseDictionaryLookupVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
