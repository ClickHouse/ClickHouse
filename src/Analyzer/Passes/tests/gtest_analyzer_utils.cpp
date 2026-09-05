#include <Analyzer/Passes/tests/gtest_analyzer_utils.h>
#include <gtest/gtest.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/IQueryTreePass.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

/// Resolve columns of a type within a query tree
static QueryTreeNodePtr resolveColumn(
    DataTypePtr type,
    QueryTreeNodePtr node,
    std::map<String, QueryTreeNodePtr> & resolved_map,
    std::map<String, TableExpressionNodePtr> & source_map,
    ContextPtr context)
{
    auto * function_node = node->as<FunctionNode>();
    if (!function_node)
    {
        auto * identifier_node = node->as<IdentifierNode>();
        /// it is a column
        if (identifier_node)
        {
            String col_name = identifier_node->getIdentifier().getFullName();
            auto it = resolved_map.find(col_name);
            if (it != resolved_map.end())
                return it->second;
            /// One shared source per qualifier (`t1.a`, `t1.b` -> `t1`); the column keeps
            /// only the last identifier part as its name. The sources are owned by `source_map`
            /// (not by the tree), so they stay alive while the pass runs.
            String short_name = identifier_node->getIdentifier().back();
            Identifier qualifier = identifier_node->getIdentifier();
            qualifier.popLast();
            String source_name = qualifier.empty() ? "__default_table" : qualifier.getFullName();
            auto & source = source_map[source_name];
            if (!source)
                source = std::make_shared<TableFunctionNode>(source_name);
            auto column = std::make_shared<ColumnNode>(NameAndTypePair(short_name, type), source);
            resolved_map[col_name] = column;
            return column;
        }
        /// it is constant
        return node;
    }
    QueryTreeNodes new_args;
    for (const auto & argument : function_node->getArguments())
    {
        auto arg = resolveColumn(type, argument, resolved_map, source_map, context);
        new_args.push_back(arg);
    }
    function_node->getArguments().getNodes() = std::move(new_args);
    resolveOrdinaryFunctionNodeByName(*function_node, function_node->getFunctionName(), context);
    return node;
};

/// Run an analyzer pass on a condition involving a column of a type, then compare against the expected expression.
void testPassOnCondition(QueryTreePassPtr pass, DataTypePtr columnType, const String & cond, const String & expected)
{
    ContextPtr context = getContext().context; ParserExpressionWithOptionalAlias exp_elem(false);
    ASTPtr query = parseQuery(exp_elem, cond, 10000, 10000, 10000);
    QueryTreeNodePtr node = buildQueryTree(query, context);

    std::map<String, QueryTreeNodePtr> resolved_map;
    std::map<String, TableExpressionNodePtr> source_map;
    node = resolveColumn(columnType, node, resolved_map, source_map, context);
    pass->run(node, context);
    EXPECT_EQ(node->formatConvertedASTForErrorMessage(), expected);
}
