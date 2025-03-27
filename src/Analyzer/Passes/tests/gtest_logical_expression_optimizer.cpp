#include <gtest/gtest.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/Passes/LogicalExpressionOptimizerPass.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

QueryTreeNodePtr resolve_everything(QueryTreeNodePtr node, std::map<String, QueryTreeNodePtr> & resolved_map, ContextPtr context)
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
            DataTypePtr type = DataTypePtr(new DataTypeInt32());
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
        auto arg = resolve_everything(argument, resolved_map, context);
        new_args.push_back(arg);
    }
    function_node->getArguments().getNodes() = std::move(new_args);
    resolveOrdinaryFunctionNodeByName(*function_node, function_node->getFunctionName(), context);
    return node;
};

TEST(OptimizeAndCompareChain, compare)
{
    tryRegisterFunctions();
    std::map<String, QueryTreeNodePtr> resolved_map;
    auto test_f = [&](const String & cond, const String & result)
    {
        ContextPtr context = getContext().context;
        ParserExpressionWithOptionalAlias exp_elem(false);
        ASTPtr query = parseQuery(exp_elem, cond, 10000, 10000, 10000);
        QueryTreeNodePtr node = buildQueryTree(query, context);

        node = resolve_everything(node, resolved_map, context);
        LogicalExpressionOptimizerPass pass;
        pass.run(node, context);
        EXPECT_EQ(node->formatConvertedASTForErrorMessage(), result);
    };

    // constant is large
    test_f("a < b AND b < c AND c < 5", "(a < b) AND (b < c) AND (c < 5) AND (b < 5) AND (a < 5)");
    test_f("b > a AND c > b AND 5 > c", "(b > a) AND (c > b) AND (5 > c) AND (b < 5) AND (a < 5)");
    test_f("a <= b AND b <= c AND c <= 5", "(a <= b) AND (b <= c) AND (c <= 5) AND (b <= 5) AND (a <= 5)");
    test_f("a < b AND b < c AND c <= 5", "(a < b) AND (b < c) AND (c <= 5) AND (b < 5) AND (a < 5)");
    test_f("a < b AND b <= c AND c < 5", "(a < b) AND (b <= c) AND (c < 5) AND (b < 5) AND (a < 5)");
    test_f("b >= a AND c >= b AND 5 >= c", "(b >= a) AND (c >= b) AND (5 >= c) AND (b <= 5) AND (a <= 5)");
    test_f("b > a AND c > b AND 5 >= c", "(b > a) AND (c > b) AND (5 >= c) AND (b < 5) AND (a < 5)");
    test_f("b > a AND c >= b AND 5 > c", "(b > a) AND (c >= b) AND (5 > c) AND (b < 5) AND (a < 5)");
    test_f("a = b AND b = c AND c < 5", "(a = b) AND (b = c) AND (c < 5) AND (b < 5) AND (a < 5)");
    test_f("a < b AND b = c AND c <= 5", "(a < b) AND (b = c) AND (c <= 5) AND (b <= 5) AND (a < 5)");
    test_f("a < b AND b = c AND c = 5", "(a < b) AND (b = c) AND (c = 5) AND (b = 5) AND (a < 5)");
    test_f("a > b AND b > c AND c > a AND a < 5", "(a > b) AND (b > c) AND (c > a) AND (a < 5) AND (b < 5) AND (c < 5)");
    test_f("a < 3 AND b < a AND c < b AND c < a", "(a < 3) AND (b < a) AND (c < b) AND (c < a) AND (b < 3) AND (c < 3)");

    // constant is small
    test_f("a > b AND b > c AND c > 5", "(a > b) AND (b > c) AND (c > 5) AND (b > 5) AND (a > 5)");
    test_f("b < a AND c < b AND 5 < c", "(b < a) AND (c < b) AND (5 < c) AND (b > 5) AND (a > 5)");
    test_f("a >= b AND b >= c AND c >= 5", "(a >= b) AND (b >= c) AND (c >= 5) AND (b >= 5) AND (a >= 5)");
    test_f("a > b AND b > c AND c >= 5", "(a > b) AND (b > c) AND (c >= 5) AND (b > 5) AND (a > 5)");
    test_f("a > b AND b >= c AND c > 5", "(a > b) AND (b >= c) AND (c > 5) AND (b > 5) AND (a > 5)");
    test_f("b <= a AND c <= b AND 5 <= c", "(b <= a) AND (c <= b) AND (5 <= c) AND (b >= 5) AND (a >= 5)");
    test_f("b < a AND c < b AND 5 <= c", "(b < a) AND (c < b) AND (5 <= c) AND (b > 5) AND (a > 5)");
    test_f("b < a AND c <= b AND 5 < c", "(b < a) AND (c <= b) AND (5 < c) AND (b > 5) AND (a > 5)");
    test_f("a = b AND b = c AND c > 5", "(a = b) AND (b = c) AND (c > 5) AND (b > 5) AND (a > 5)");
    test_f("a > b AND b = c AND c >= 5", "(a > b) AND (b = c) AND (c >= 5) AND (b >= 5) AND (a > 5)");
    test_f("a > b AND b = c AND c = 5", "(a > b) AND (b = c) AND (c = 5) AND (b = 5) AND (a > 5)");
    test_f("a < b AND b < c AND c < a AND a > 5", "(a < b) AND (b < c) AND (c < a) AND (a > 5) AND (b > 5) AND (c > 5)");
    test_f("a > 3 AND b > a AND c > b AND c > a", "(a > 3) AND (b > a) AND (c > b) AND (c > a) AND (b > 3) AND (c > 3)");

    // miscellaneous
    test_f("c > 0 AND c < 5", "(c > 0) AND (c < 5)");
    test_f("a = b AND b = c AND c = 5", "(a = b) AND (b = c) AND (c = 5) AND (b = 5) AND (a = 5)");
    test_f("c < b AND a < 5 AND b < 6 AND b < 5", "(c < b) AND (a < 5) AND (b < 6) AND (b < 5) AND (c < 6) AND (c < 5)");
    test_f("a = b AND a > 3 AND b > 0", "(a = b) AND (a > 3) AND (b > 0) AND (a > 0) AND (b > 3)");
}
