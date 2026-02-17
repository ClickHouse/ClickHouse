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
    test_f("a < b AND b < c AND c < 5", "and(less(a, b), less(b, c), less(c, 5), less(b, 5), less(a, 5))");
    test_f("b > a AND c > b AND 5 > c", "and(greater(b, a), greater(c, b), greater(5, c), less(b, 5), less(a, 5))");
    test_f("a <= b AND b <= c AND c <= 5", "and(lessOrEquals(a, b), lessOrEquals(b, c), lessOrEquals(c, 5), lessOrEquals(b, 5), lessOrEquals(a, 5))");
    test_f("a < b AND b < c AND c <= 5", "and(less(a, b), less(b, c), lessOrEquals(c, 5), less(b, 5), less(a, 5))");
    test_f("a < b AND b <= c AND c < 5", "and(less(a, b), lessOrEquals(b, c), less(c, 5), less(b, 5), less(a, 5))");
    test_f("b >= a AND c >= b AND 5 >= c", "and(greaterOrEquals(b, a), greaterOrEquals(c, b), greaterOrEquals(5, c), lessOrEquals(b, 5), lessOrEquals(a, 5))");
    test_f("b > a AND c > b AND 5 >= c", "and(greater(b, a), greater(c, b), greaterOrEquals(5, c), less(b, 5), less(a, 5))");
    test_f("b > a AND c >= b AND 5 > c", "and(greater(b, a), greaterOrEquals(c, b), greater(5, c), less(b, 5), less(a, 5))");
    test_f("a = b AND b = c AND c < 5", "and(equals(a, b), equals(b, c), less(c, 5), less(b, 5), less(a, 5))");
    test_f("a < b AND b = c AND c <= 5", "and(less(a, b), equals(b, c), lessOrEquals(c, 5), lessOrEquals(b, 5), less(a, 5))");
    test_f("a < b AND b = c AND c = 5", "and(less(a, b), equals(b, c), equals(c, 5), equals(b, 5), less(a, 5))");
    test_f("a > b AND b > c AND c > a AND a < 5", "and(greater(a, b), greater(b, c), greater(c, a), less(a, 5), less(b, 5), less(c, 5))");
    test_f("a < 3 AND b < a AND c < b AND c < a", "and(less(a, 3), less(b, a), less(c, b), less(c, a), less(b, 3), less(c, 3))");

    // constant is small
    test_f("a > b AND b > c AND c > 5", "and(greater(a, b), greater(b, c), greater(c, 5), greater(b, 5), greater(a, 5))");
    test_f("b < a AND c < b AND 5 < c", "and(less(b, a), less(c, b), less(5, c), greater(b, 5), greater(a, 5))");
    test_f("a >= b AND b >= c AND c >= 5", "and(greaterOrEquals(a, b), greaterOrEquals(b, c), greaterOrEquals(c, 5), greaterOrEquals(b, 5), greaterOrEquals(a, 5))");
    test_f("a > b AND b > c AND c >= 5", "and(greater(a, b), greater(b, c), greaterOrEquals(c, 5), greater(b, 5), greater(a, 5))");
    test_f("a > b AND b >= c AND c > 5", "and(greater(a, b), greaterOrEquals(b, c), greater(c, 5), greater(b, 5), greater(a, 5))");
    test_f("b <= a AND c <= b AND 5 <= c", "and(lessOrEquals(b, a), lessOrEquals(c, b), lessOrEquals(5, c), greaterOrEquals(b, 5), greaterOrEquals(a, 5))");
    test_f("b < a AND c < b AND 5 <= c", "and(less(b, a), less(c, b), lessOrEquals(5, c), greater(b, 5), greater(a, 5))");
    test_f("b < a AND c <= b AND 5 < c", "and(less(b, a), lessOrEquals(c, b), less(5, c), greater(b, 5), greater(a, 5))");
    test_f("a = b AND b = c AND c > 5", "and(equals(a, b), equals(b, c), greater(c, 5), greater(b, 5), greater(a, 5))");
    test_f("a > b AND b = c AND c >= 5", "and(greater(a, b), equals(b, c), greaterOrEquals(c, 5), greaterOrEquals(b, 5), greater(a, 5))");
    test_f("a > b AND b = c AND c = 5", "and(greater(a, b), equals(b, c), equals(c, 5), equals(b, 5), greater(a, 5))");
    test_f("a < b AND b < c AND c < a AND a > 5", "and(less(a, b), less(b, c), less(c, a), greater(a, 5), greater(b, 5), greater(c, 5))");
    test_f("a > 3 AND b > a AND c > b AND c > a", "and(greater(a, 3), greater(b, a), greater(c, b), greater(c, a), greater(b, 3), greater(c, 3))");

    // miscellaneous
    test_f("c > 0 AND c < 5", "and(greater(c, 0), less(c, 5))");
    test_f("a = b AND b = c AND c = 5", "and(equals(a, b), equals(b, c), equals(c, 5), equals(b, 5), equals(a, 5))");
    test_f("c < b AND a < 5 AND b < 6 AND b < 5", "and(less(c, b), less(a, 5), less(b, 6), less(b, 5), less(c, 6), less(c, 5))");
    test_f("a = b AND a > 3 AND b > 0", "and(equals(a, b), greater(a, 3), greater(b, 0), greater(a, 0), greater(b, 3))");
    test_f("(3 < a AND a < 5) AND b < a AND c > a", "and(and(less(3, a), less(a, 5)), less(b, a), greater(c, a), less(b, 5), greater(c, 3))");
}
