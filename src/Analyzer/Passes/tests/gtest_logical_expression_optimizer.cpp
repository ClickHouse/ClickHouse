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
    test_f("a < b AND b < c AND c < 5", "(a < b) AND (b < c) AND (c < 5) AND indexHint(b < 5) AND indexHint(a < 5)");
    test_f("b > a AND c > b AND 5 > c", "(b > a) AND (c > b) AND (5 > c) AND indexHint(b < 5) AND indexHint(a < 5)");
    test_f("a <= b AND b <= c AND c <= 5", "(a <= b) AND (b <= c) AND (c <= 5) AND indexHint(b <= 5) AND indexHint(a <= 5)");
    test_f("a < b AND b < c AND c <= 5", "(a < b) AND (b < c) AND (c <= 5) AND indexHint(b < 5) AND indexHint(a < 5)");
    test_f("a < b AND b <= c AND c < 5", "(a < b) AND (b <= c) AND (c < 5) AND indexHint(b < 5) AND indexHint(a < 5)");
    test_f("b >= a AND c >= b AND 5 >= c", "(b >= a) AND (c >= b) AND (5 >= c) AND indexHint(b <= 5) AND indexHint(a <= 5)");
    test_f("b > a AND c > b AND 5 >= c", "(b > a) AND (c > b) AND (5 >= c) AND indexHint(b < 5) AND indexHint(a < 5)");
    test_f("b > a AND c >= b AND 5 > c", "(b > a) AND (c >= b) AND (5 > c) AND indexHint(b < 5) AND indexHint(a < 5)");
    test_f("a = b AND b = c AND c < 5", "(a = b) AND (b = c) AND (c < 5) AND indexHint(b < 5) AND indexHint(a < 5)");
    test_f("a < b AND b = c AND c <= 5", "(a < b) AND (b = c) AND (c <= 5) AND indexHint(b <= 5) AND indexHint(a < 5)");
    test_f("a < b AND b = c AND c = 5", "(a < b) AND (b = c) AND (c = 5) AND indexHint(b = 5) AND indexHint(a < 5)");
    test_f("a > b AND b > c AND c > a AND a < 5", "(a > b) AND (b > c) AND (c > a) AND (a < 5) AND indexHint(b < 5) AND indexHint(c < 5)");
    test_f("a < 3 AND b < a AND c < b AND c < a", "(a < 3) AND (b < a) AND (c < b) AND (c < a) AND indexHint(b < 3) AND indexHint(c < 3)");

    // constant is small
    test_f("a > b AND b > c AND c > 5", "(a > b) AND (b > c) AND (c > 5) AND indexHint(b > 5) AND indexHint(a > 5)");
    test_f("b < a AND c < b AND 5 < c", "(b < a) AND (c < b) AND (5 < c) AND indexHint(b > 5) AND indexHint(a > 5)");
    test_f("a >= b AND b >= c AND c >= 5", "(a >= b) AND (b >= c) AND (c >= 5) AND indexHint(b >= 5) AND indexHint(a >= 5)");
    test_f("a > b AND b > c AND c >= 5", "(a > b) AND (b > c) AND (c >= 5) AND indexHint(b > 5) AND indexHint(a > 5)");
    test_f("a > b AND b >= c AND c > 5", "(a > b) AND (b >= c) AND (c > 5) AND indexHint(b > 5) AND indexHint(a > 5)");
    test_f("b <= a AND c <= b AND 5 <= c", "(b <= a) AND (c <= b) AND (5 <= c) AND indexHint(b >= 5) AND indexHint(a >= 5)");
    test_f("b < a AND c < b AND 5 <= c", "(b < a) AND (c < b) AND (5 <= c) AND indexHint(b > 5) AND indexHint(a > 5)");
    test_f("b < a AND c <= b AND 5 < c", "(b < a) AND (c <= b) AND (5 < c) AND indexHint(b > 5) AND indexHint(a > 5)");
    test_f("a = b AND b = c AND c > 5", "(a = b) AND (b = c) AND (c > 5) AND indexHint(b > 5) AND indexHint(a > 5)");
    test_f("a > b AND b = c AND c >= 5", "(a > b) AND (b = c) AND (c >= 5) AND indexHint(b >= 5) AND indexHint(a > 5)");
    test_f("a > b AND b = c AND c = 5", "(a > b) AND (b = c) AND (c = 5) AND indexHint(b = 5) AND indexHint(a > 5)");
    test_f("a < b AND b < c AND c < a AND a > 5", "(a < b) AND (b < c) AND (c < a) AND (a > 5) AND indexHint(b > 5) AND indexHint(c > 5)");
    test_f("a > 3 AND b > a AND c > b AND c > a", "(a > 3) AND (b > a) AND (c > b) AND (c > a) AND indexHint(b > 3) AND indexHint(c > 3)");

    // miscellaneous
    test_f("c > 0 AND c < 5", "(c > 0) AND (c < 5)");
    test_f("a = b AND b = c AND c = 5", "(a = b) AND (b = c) AND (c = 5) AND indexHint(b = 5) AND indexHint(a = 5)");
    /// The weaker derived `c < 6` is appended before `c < 5` prunes it inside the filter map;
    /// the later pruning pass does not look inside `indexHint`, so the redundant hint stays.
    test_f("c < b AND a < 5 AND b < 6 AND b < 5", "(c < b) AND (a < 5) AND (b < 5) AND indexHint(c < 6) AND indexHint(c < 5)");
    /// `b > 0` used to be dropped as implied by the plain derived `b > 3`; a hint does not imply it.
    test_f("a = b AND a > 3 AND b > 0", "(a = b) AND (a > 3) AND (b > 0) AND indexHint(b > 3)");
    test_f("(3 < a AND a < 5) AND b < a AND c > a", "((3 < a) AND (a < 5)) AND (b < a) AND (c > a) AND indexHint(b < 5) AND indexHint(c > 3)");

    /// A derived comparison that contradicts an existing condition is appended as a plain
    /// condition (not a hint), so the pruning pass folds the whole AND to `false`.
    test_f("a < b AND b < 5 AND a > 10", "false");
}
