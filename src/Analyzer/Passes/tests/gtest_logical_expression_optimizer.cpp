#include <gtest/gtest.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/Passes/LogicalExpressionOptimizerPass.h>
#include <Analyzer/Passes/tests/gtest_analyzer_utils.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

TEST(OptimizeAndCompareChain, compare)
{
    tryRegisterFunctions();
    auto test_f = [&](const String & cond, const String & expected)
    {
        testPassOnCondition(
            QueryTreePassPtr(new LogicalExpressionOptimizerPass()), DataTypePtr(new DataTypeInt32()), 
            cond, expected);
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
    test_f("(3 < a AND a < 5) AND b < a AND c > a", "((3 < a) AND (a < 5)) AND (b < a) AND (c > a) AND (b < 5) AND (c > 3)");
}
