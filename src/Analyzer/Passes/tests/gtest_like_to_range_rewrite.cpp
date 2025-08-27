#include <DataTypes/DataTypeString.h>
#include <gtest/gtest.h>

#include <Analyzer/Passes/LikeToRangeRewritePass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

QueryTreeNodePtr resolve_string_expr(QueryTreeNodePtr node, std::map<String, QueryTreeNodePtr> & resolved_map, ContextPtr context)
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
            DataTypePtr type = DataTypePtr(new DataTypeString());
            auto column = std::make_shared<ColumnNode>(NameAndTypePair(col_name, type), node);
            resolved_map[col_name] = column;
            return column;
        }
        /// it is constant
        return node;
    }
    QueryTreeNodes new_args;
    for (const auto & argument : function_node->getArguments())
    {
        auto arg = resolve_string_expr(argument, resolved_map, context);
        new_args.push_back(arg);
    }
    function_node->getArguments().getNodes() = std::move(new_args);
    resolveOrdinaryFunctionNodeByName(*function_node, function_node->getFunctionName(), context);
    return node;
};

TEST(LikeToRangeRewrite, rewrite)
{
    tryRegisterFunctions();
    std::map<String, QueryTreeNodePtr> resolved_map;
    auto test_f = [&](const String & cond, const String & expected)
    {
        ContextPtr context = getContext().context;
        ParserExpressionWithOptionalAlias exp_elem(false);
        ASTPtr query = parseQuery(exp_elem, cond, 10000, 10000, 10000);
        QueryTreeNodePtr node = buildQueryTree(query, context);

        node = resolve_string_expr(node, resolved_map, context);
        LikeToRangeRewritePass pass;
        pass.run(node, context);
        EXPECT_EQ(node->formatConvertedASTForErrorMessage(), expected);
    };

    /// Perfect prefix LIKE
    test_f("col LIKE 'Test%'", "(col >= 'Test') AND (col < 'Tesu')");
    test_f("col LIKE 'a%'", "(col >= 'a') AND (col < 'b')");

    /// Perfect prefix ILIKE
    test_f("col ILIKE 'Test%'", "(lower(col) >= 'test') AND (lower(col) < 'tesu')");
    test_f("col ILIKE 'A%'", "(lower(col) >= 'a') AND (lower(col) < 'b')");

    /// Perfect prefix without right bound
    test_f("col LIKE '\xFF%'", "col >= '\xFF'");
    test_f("col ILIKE '\xFF%'", "lower(col) >= '\xFF'");

    /// Perfect prefix NOT (I)LIKE
    test_f("col NOT LIKE 'Test%'", "(col < 'Test') OR (col >= 'Tesu')");
    test_f("col NOT ILIKE 'Test%'", "(lower(col) < 'test') OR (lower(col) >= 'tesu')");

    /// Imperfect prefix (I)LIKE should not be rewritten
    test_f("col LIKE 'hello_world%'", "col LIKE 'hello_world%'");
    test_f("col LIKE '%test%'", "col LIKE '%test%'");
    test_f("col LIKE '%test'", "col LIKE '%test'");
    test_f("col LIKE '_test%'", "col LIKE '_test%'");
    test_f("col LIKE '%'", "col LIKE '%'");
    test_f("col LIKE 'exactvalue'", "col LIKE 'exactvalue'");

    test_f("col ILIKE 'hello_world%'", "col ILIKE 'hello_world%'");

    /// Imperfect prefix NOT (I)LIKE should not be rewritten
    test_f("col NOT LIKE 'hello_world%'", "col NOT LIKE 'hello_world%'");
    test_f("col NOT ILIKE 'hello_world%'", "col NOT ILIKE 'hello_world%'");
}
