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
#include <Interpreters/ExternalDictionariesLoader.h>

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

bool isDictGetWithoutDefault(const String & name)
{
    if (!name.starts_with("dictGet"))
        return false;

    return name.find("OrDefault") == String::npos;
}

bool tryParseDictFunctionCall(const QueryTreeNodePtr & node, DictGetFunctionInfo & out)
{
    const auto * function_node = node->as<FunctionNode>();
    if (!function_node || !isDictGetWithoutDefault(function_node->getFunctionName()))
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

bool tryGetConstantNode(const QueryTreeNodePtr & node, QueryTreeNodePtr & out)
{
    if (node->as<ConstantNode>())
    {
        out = node;
        return true;
    }
    return false;
}

bool isInMemoryLayout(const String & type_name)
{
    return type_name == "Flat" || type_name == "Hashed" || type_name == "ComplexKeyHashed" || type_name == "RangeHashed";
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
    [[maybe_unused]] void rewriteDictGetPredicateRecursively(QueryTreeNodePtr & node)
    {
        auto * node_function = node->as<FunctionNode>();

        if (!node_function)
            return;

        /// If its and/or/not, recurse into arguments
        const auto & function_name = node_function->getFunctionName();

        if (function_name == "and" || function_name == "or" || function_name == "not")
        {
            for (auto & argument : node_function->getArguments().getNodes())
                rewriteDictGetPredicateRecursively(argument);
            return;
        }

        static std::unordered_set<String> allowed_comparison_functions
            = {"equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals", "like", "notLike", "ilike", "notILike"};

        const String attr_comparison_function_name = node_function->getFunctionName();
        if (!allowed_comparison_functions.contains(attr_comparison_function_name))
            return;

        auto & arguments = node_function->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        DictGetFunctionInfo dictget_function_info{};
        QueryTreeNodePtr const_value_expr_node{};

        if (!(tryParseDictFunctionCall(arguments[0], dictget_function_info) && tryGetConstantNode(arguments[1], const_value_expr_node))
            && !(tryParseDictFunctionCall(arguments[1], dictget_function_info) && tryGetConstantNode(arguments[0], const_value_expr_node)))
            return;

        std::vector<NameAndTypePair> key_cols;
        DataTypePtr dict_attr_col_type;

        try
        {
            const auto & loader = getContext()->getExternalDictionariesLoader();
            auto dict = loader.getDictionary(dictget_function_info.dict_name, getContext());
            if (!dict)
                return;

            const String dict_type_name = dict->getTypeName();

            if (!isInMemoryLayout(dict_type_name))
                return;

            const auto & dict_structure = dict->getStructure();

            if (dict_structure.id)
            {
                key_cols.emplace_back(dict_structure.id->name, dict_structure.id->type);
            }
            else if (dict_structure.key)
            {
                for (const auto & id : *dict_structure.key)
                {
                    key_cols.emplace_back(id.name, id.type);
                }
            }
            else
            {
                return;
            }

            assert(
                dict_structure.hasAttribute(dictget_function_info.attr_col_name)
                && "Attribute not found in dictionary structure of dictionary");

            dict_attr_col_type = dict_structure.getAttribute(dictget_function_info.attr_col_name).type;
        }
        catch (...)
        {
            return;
        }

        auto dict_table_function = std::make_shared<TableFunctionNode>("dictionary");
        dict_table_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(dictget_function_info.dict_name));
        resolveNode(dict_table_function, getContext());

        NameAndTypePair attr_col{dictget_function_info.attr_col_name, dict_attr_col_type};
        auto attr_col_node = std::make_shared<ColumnNode>(attr_col, dict_table_function);

        QueryTreeNodePtr attr_col_node_casted = attr_col_node;
        if (!attr_col_node->getResultType()->equals(*dictget_function_info.return_type))
        {
            attr_col_node_casted = createCastFunction(attr_col_node, dictget_function_info.return_type, getContext());
        }

        QueryTreeNodes key_col_nodes;
        for (const auto & key_col : key_cols)
        {
            key_col_nodes.push_back(std::make_shared<ColumnNode>(key_col, dict_table_function));
        }

        auto attr_comparison_function_node = std::make_shared<FunctionNode>(attr_comparison_function_name);
        attr_comparison_function_node->markAsOperator();
        attr_comparison_function_node->getArguments().getNodes()
            = {attr_col_node_casted, const_value_expr_node}; // literal node needs to be more general cannot be string
        resolveOrdinaryFunctionNodeByName(*attr_comparison_function_node, attr_comparison_function_name, getContext());

        // SELECT id FROM dictionary('colors') WHERE name = <literal>
        auto subquery_node = std::make_shared<QueryNode>(Context::createCopy(getContext()));
        subquery_node->getJoinTree() = dict_table_function;
        subquery_node->getWhere() = attr_comparison_function_node;

        for (const auto & key_col_node : key_col_nodes)
        {
            subquery_node->getProjection().getNodes().push_back(key_col_node);
        }
        subquery_node->resolveProjectionColumns(key_cols);
        resolveNode(subquery_node, getContext());

        auto in_function_node = std::make_shared<FunctionNode>("in");
        in_function_node->markAsOperator();
        QueryTreeNodePtr querytree_subquery_node = subquery_node;
        in_function_node->getArguments().getNodes() = {dictget_function_info.key_expr_node, querytree_subquery_node};
        resolveOrdinaryFunctionNodeByName(*in_function_node, "in", getContext());

        if (!node_function->getResultType()->equals(*in_function_node->getResultType()))
            node = createCastFunction(in_function_node, node_function->getResultType(), getContext());
        else
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
