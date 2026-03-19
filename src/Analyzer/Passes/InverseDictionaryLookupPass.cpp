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

#include <Functions/FunctionsExternalDictionaries.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace Setting
{
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
           "dictGetIPv6",
           "dictGetOrNull"};

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

template <typename Node>
void resolveNode(const Node & node, const ContextPtr & context)
{
    if (node->isResolved())
        return;

    QueryTreeNodePtr querytree_node = node;
    QueryAnalysisPass(/*only_analyze*/ false).run(querytree_node, context);
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

        auto * node_function = node->as<FunctionNode>();

        if (!node_function)
            return;

        const String function_name = node_function->getFunctionName();

        static std::unordered_set<String> allowed_in_functions = {"in", "notIn"};
        static std::unordered_set<String> allowed_comparison_functions = {
            "equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals", "like", "notLike", "ilike", "notILike", "match"};

        bool is_in_function = allowed_in_functions.contains(function_name);
        bool is_comparison_function = allowed_comparison_functions.contains(function_name);

        if (!is_in_function && !is_comparison_function)
            return;

        auto ctx = prepareTransformContext(node_function, is_in_function);
        if (!ctx)
            return;

        auto where_condition = buildWhereCondition(*ctx, node_function, function_name, is_in_function);
        auto subquery = buildSubquery(*ctx, where_condition);

        /// For comparison predicates, always use "in" as the final function
        /// For IN predicates, preserve the original function name (in/notIn)
        const String result_function_name = is_in_function ? function_name : "in";
        auto final_in_expr = buildFinalInExpression(*ctx, subquery, result_function_name);

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
    };

    /// Validates the function arguments and extracts dictionary/attribute information.
    /// Returns nullopt if the pattern doesn't match or dictionary is not suitable.
    std::optional<TransformContext> prepareTransformContext(FunctionNode * node_function, bool is_in_function)
    {
        auto & arguments = node_function->getArguments().getNodes();
        if (arguments.size() != 2)
            return std::nullopt;

        DictSide side;
        DictGetFunctionInfo dictget_info;
        QueryTreeNodePtr constant_arg;

        if (is_in_function)
        {
            auto info = tryParseDictFunctionCall(arguments[0]);
            if (!info)
                return std::nullopt;
            if (!arguments[1]->as<ConstantNode>())
                return std::nullopt;
            side = DictSide::LHS;
            dictget_info = std::move(*info);
            constant_arg = arguments[1];
        }
        else
        {
            if (auto info_lhs = tryParseDictFunctionCall(arguments[0]); info_lhs && arguments[1]->as<ConstantNode>())
            {
                side = DictSide::LHS;
                dictget_info = std::move(*info_lhs);
                constant_arg = arguments[1];
            }
            else if (auto info_rhs = tryParseDictFunctionCall(arguments[1]); info_rhs && arguments[0]->as<ConstantNode>())
            {
                side = DictSide::RHS;
                dictget_info = std::move(*info_rhs);
                constant_arg = arguments[0];
            }
            else
            {
                return std::nullopt;
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

        const String attr_col_name = dictget_info.attr_col_name_node->getValue().safeGet<String>();
        if (!dict_structure.hasAttribute(attr_col_name))
            return std::nullopt;

        DataTypePtr dict_attr_col_type = dict_structure.getAttribute(attr_col_name).type;

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
        };
    }

    /// Builds the WHERE condition for the subquery.
    /// For IN: builds attr_col IN (consts)
    /// For comparison: builds attr_col op const (respecting side for argument order)
    QueryTreeNodePtr
    buildWhereCondition(const TransformContext & ctx, FunctionNode * original_function, const String & function_name, bool is_in_function)
    {
        auto where_function_node = std::static_pointer_cast<FunctionNode>(original_function->clone());
        where_function_node->markAsOperator();

        if (is_in_function)
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

    QueryTreeNodePtr
    buildFinalInExpression(const TransformContext & ctx, const QueryTreeNodePtr & subquery, const String & result_function_name)
    {
        auto in_function_node = std::make_shared<FunctionNode>(result_function_name);
        in_function_node->markAsOperator();
        in_function_node->getArguments().getNodes() = {ctx.dictget_info.key_expr_node, subquery};
        resolveOrdinaryFunctionNodeByName(*in_function_node, result_function_name, getContext());
        return in_function_node;
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
