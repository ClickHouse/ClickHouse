#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/InverseDictionaryLookupPass.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Processors/QueryPlan/resolveStorages.h>


#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
extern const SettingsBool optimize_inverse_dictionary_lookup;
}

namespace
{


/** Optimize single `dictGet = LITERAL` into `IN [array keys where value = LITERAL]` subquery
  *
  * Example: SELECT col FROM tab WHERE dictGet(DICT_NAME, DICT_VAL_COL, col) = LITERAL;
  * Result: SELECT col FROM t WHERE col IN (SELECT DICT_KEY_COL FROM dictionary(DICT_NAME) WHERE DICT_VAL_COL = LITERAL);
  */

struct DictGetCall
{
    String dict_name;
    String attr_col_name;
    QueryTreeNodePtr key_expr;
    DataTypePtr result_type;
};

bool isStringLiteral(const QueryTreeNodePtr & node, String & out)
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

bool isDictGetFamily(const String & name)
{
    if (!name.starts_with("dictGet"))
        return false;

    return name.find("OrDefault") == String::npos;
}

bool parseDictGetLike(const QueryTreeNodePtr & node, DictGetCall & out)
{
    const auto * function_node = node->as<FunctionNode>();
    if (!function_node || !isDictGetFamily(function_node->getFunctionName()))
        return false;

    const auto & arguments = function_node->getArguments().getNodes();

    if (arguments.size() != 3)
        return false;

    String dict_name;
    String attr_col_name;
    if (!isStringLiteral(arguments[0], dict_name) || !isStringLiteral(arguments[1], attr_col_name))
        return false;

    out.dict_name = std::move(dict_name);
    out.attr_col_name = std::move(attr_col_name);
    out.key_expr = arguments[2];
    out.result_type = function_node->getResultType();
    return true;
}

bool isConstantNode(const QueryTreeNodePtr & node, QueryTreeNodePtr & out)
{
    if (node->as<ConstantNode>())
    {
        out = node;
        return true;
    }
    return false;
}

bool isInMemoryDictionary(const String & type_name)
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
                recursivelyRewriteDictGetEqual(q->getWhere());
            if (q->hasPrewhere())
                recursivelyRewriteDictGetEqual(q->getPrewhere());
            if (q->hasQualify())
                recursivelyRewriteDictGetEqual(q->getQualify());
        }
    }

private:
    [[maybe_unused]] void recursivelyRewriteDictGetEqual(QueryTreeNodePtr & node)
    {
        auto * node_function = node->as<FunctionNode>();

        if (!node_function)
            return;

        /// If its and/or/not, recurse into arguments
        const auto & function_name = node_function->getFunctionName();

        if (function_name == "and" || function_name == "or" || function_name == "not")
        {
            for (auto & argument : node_function->getArguments().getNodes())
                recursivelyRewriteDictGetEqual(argument);
            return;
        }

        static std::unordered_set<String> allowed_comparison_functions
            = {"equals", "notEquals", "less", "lessOrEquals", "greater", "greaterOrEquals", "like", "notLike", "ilike", "notILike"};

        const String comparison_function_name = node_function->getFunctionName();
        if (!allowed_comparison_functions.contains(comparison_function_name))
            return;

        auto & arguments = node_function->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        DictGetCall dictget_call{};
        QueryTreeNodePtr value_expr_node{};

        if (!(parseDictGetLike(arguments[0], dictget_call) && isConstantNode(arguments[1], value_expr_node))
            && !(parseDictGetLike(arguments[1], dictget_call) && isConstantNode(arguments[0], value_expr_node)))
            return;

        std::vector<NameAndTypePair> key_cols;
        DataTypePtr dict_attr_col_type;

        try
        {
            const auto & loader = getContext()->getExternalDictionariesLoader();
            auto dict = loader.getDictionary(dictget_call.dict_name, getContext());
            if (!dict)
                return;

            const String dict_type_name = dict->getTypeName();

            if (!isInMemoryDictionary(dict_type_name))
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

            assert(dict_structure.hasAttribute(dictget_call.attr_col_name) && "Attribute not found in dictionary structure of dictionary");

            dict_attr_col_type = dict_structure.getAttribute(dictget_call.attr_col_name).type;
        }
        catch (...)
        {
            return;
        }

        auto dict_table_function = std::make_shared<TableFunctionNode>("dictionary");
        dict_table_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(dictget_call.dict_name));
        resolveNode(dict_table_function, getContext());

        NameAndTypePair attr_col{dictget_call.attr_col_name, dict_attr_col_type};
        auto attr_col_node = std::make_shared<ColumnNode>(attr_col, dict_table_function);

        QueryTreeNodePtr querytree_attr_col_node = attr_col_node;
        if (!attr_col_node->getResultType()->equals(*dictget_call.result_type))
        {
            querytree_attr_col_node = createCastFunction(attr_col_node, dictget_call.result_type, getContext());
        }

        QueryTreeNodes key_col_nodes;
        for (const auto & key_col : key_cols)
        {
            key_col_nodes.push_back(std::make_shared<ColumnNode>(key_col, dict_table_function));
        }

        auto comparison_function_node = std::make_shared<FunctionNode>(comparison_function_name);
        comparison_function_node->markAsOperator();
        comparison_function_node->getArguments().getNodes()
            = {querytree_attr_col_node, value_expr_node}; // literal node needs to be more general cannot be string
        resolveOrdinaryFunctionNodeByName(*comparison_function_node, comparison_function_name, getContext());

        // SELECT id FROM dictionary('colors') WHERE name = <literal>
        auto subquery_node = std::make_shared<QueryNode>(Context::createCopy(getContext()));
        subquery_node->getJoinTree() = dict_table_function;
        subquery_node->getWhere() = comparison_function_node;

        for (const auto & key_col_node : key_col_nodes)
        {
            subquery_node->getProjection().getNodes().push_back(key_col_node);
        }
        subquery_node->resolveProjectionColumns(key_cols);
        resolveNode(subquery_node, getContext());

        auto in_function_node = std::make_shared<FunctionNode>("in");
        in_function_node->markAsOperator();
        QueryTreeNodePtr querytree_subquery_node = subquery_node;
        in_function_node->getArguments().getNodes() = {dictget_call.key_expr, querytree_subquery_node};
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
