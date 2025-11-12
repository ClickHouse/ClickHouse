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

#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
extern const SettingsBool optimize_inverse_dictionary_lookup;
}

namespace
{

struct DictGetFunctionInfo
{
    String dict_name;
    String attr_col_name;
    QueryTreeNodePtr key_expr_node;

    /// Necessary for type casting for functions like `dictGetString`, `dictGetInt32`, etc.
    DataTypePtr return_type;
};

bool tryGetStringLiteral(const QueryTreeNodePtr & node, String & out)
{
    if (const auto * constant_node = node->as<ConstantNode>())
    {
        const auto & value = constant_node->getValue();
        if (value.getType() == Field::Types::String)
        {
            out = value.safeGet<String>();
            return true;
        }
    }
    return false;
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

bool tryParseDictFunctionCall(const QueryTreeNodePtr & node, DictGetFunctionInfo & out)
{
    const auto * function_node = node->as<FunctionNode>();
    if (!function_node || !isSupportedDictGetFunction(function_node->getFunctionName()))
        return false;

    const auto & arguments = function_node->getArguments().getNodes();

    if (arguments.size() != 3)
        return false;

    String dict_name;
    String attr_col_name;
    if (!tryGetStringLiteral(arguments[0], dict_name) || !tryGetStringLiteral(arguments[1], attr_col_name))
        return false;

    out.dict_name = std::move(dict_name);
    out.attr_col_name = std::move(attr_col_name);
    out.key_expr_node = arguments[2];
    out.return_type = function_node->getResultType();
    return true;
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

        if (auto * q = node->as<QueryNode>())
        {
            if (q->hasWhere())
                rewriteDictGetPredicateRecursively(q->getWhere());
            if (q->hasPrewhere())
                rewriteDictGetPredicateRecursively(q->getPrewhere());
            if (q->hasQualify())
                rewriteDictGetPredicateRecursively(q->getQualify());
        }
    }

private:
    void rewriteDictGetPredicateRecursively(QueryTreeNodePtr & node) const
    {
        auto * node_function = node->as<FunctionNode>();

        if (!node_function)
            return;

        const auto & function_name = node_function->getFunctionName();

        /// Some arguments might themselves be dictGet* calls, so we need to recurse into AND/OR/NOT
        if (function_name == "and" || function_name == "or" || function_name == "not")
        {
            for (auto & argument : node_function->getArguments().getNodes())
                rewriteDictGetPredicateRecursively(argument);
            return;
        }

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
        DictGetFunctionInfo dictget_function_info{};
        QueryTreeNodePtr const_value_expr_node{};

        if (tryParseDictFunctionCall(arguments[0], dictget_function_info) && tryGetConstantNode(arguments[1], const_value_expr_node))
        {
            dict_side = Side::LHS;
        }
        else if (tryParseDictFunctionCall(arguments[1], dictget_function_info) && tryGetConstantNode(arguments[0], const_value_expr_node))
        {
            dict_side = Side::RHS;
        }
        else
        {
            return;
        }

        /// Type of the attribute and key columns are not present in the query. So, we have to fetch dictionary and get the column types.
        auto helper = FunctionDictHelper(getContext());
        const auto dict = helper.getDictionary(dictget_function_info.dict_name);
        if (!dict)
            return;

        const String dict_type_name = dict->getTypeName();

        if (!isInMemoryLayout(dict_type_name))
            return;


        std::vector<NameAndTypePair> key_cols;

        const auto & dict_structure = dict->getStructure();

        if (dict_structure.id)
        {
            key_cols.emplace_back(dict_structure.id->name, dict_structure.id->type);
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

        chassert(
            dict_structure.hasAttribute(dictget_function_info.attr_col_name)
            && "Attribute not found in dictionary structure of dictionary");

        DataTypePtr dict_attr_col_type = dict_structure.getAttribute(dictget_function_info.attr_col_name).type;

        auto dict_table_function = std::make_shared<TableFunctionNode>("dictionary");
        dict_table_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(dictget_function_info.dict_name));
        resolveNode(dict_table_function, getContext());

        NameAndTypePair attr_col{dictget_function_info.attr_col_name, dict_attr_col_type};
        auto attr_col_node = std::make_shared<ColumnNode>(attr_col, dict_table_function);

        /// Needed for dictGet functions like `dictGetString`, `dictGetInt32`, etc.
        QueryTreeNodePtr attr_col_node_casted = attr_col_node;
        if (!attr_col_node->getResultType()->equals(*dictget_function_info.return_type))
        {
            attr_col_node_casted = createCastFunction(attr_col_node, dictget_function_info.return_type, getContext());
        }

        auto attr_comparison_function_node = std::make_shared<FunctionNode>(attr_comparison_function_name);
        attr_comparison_function_node->markAsOperator();
        if (dict_side == Side::LHS)
        {
            attr_comparison_function_node->getArguments().getNodes() = {attr_col_node_casted, const_value_expr_node};
        }
        else
        {
            attr_comparison_function_node->getArguments().getNodes() = {const_value_expr_node, attr_col_node_casted};
        }
        resolveOrdinaryFunctionNodeByName(*attr_comparison_function_node, attr_comparison_function_name, getContext());

        /// SELECT key_col FROM dictionary(dict_name) WHERE attr_name = const_value
        auto subquery_node = std::make_shared<QueryNode>(Context::createCopy(getContext()));
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

        node = std::move(in_function_node);
    }
};

}

void InverseDictionaryLookupPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    InverseDictionaryLookupVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
