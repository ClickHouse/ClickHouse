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
    /// The redundant weaker hint stays: the pruning pass does not look inside `indexHint`.
    test_f("c < b AND a < 5 AND b < 6 AND b < 5", "(c < b) AND (a < 5) AND (b < 5) AND indexHint(c < 6) AND indexHint(c < 5)");
    /// A hint does not imply `b > 0` away, unlike the plain derived `b > 3` before.
    test_f("a = b AND a > 3 AND b > 0", "(a = b) AND (a > 3) AND (b > 0) AND indexHint(b > 3)");
    test_f("(3 < a AND a < 5) AND b < a AND c > a", "((3 < a) AND (a < 5)) AND (b < a) AND (c > a) AND indexHint(b < 5) AND indexHint(c > 3)");

    /// A contradicting derived comparison is added plain, so the AND still folds to `false`.
    test_f("a < b AND b < 5 AND a > 10", "0");
    /// `notEquals` seeds the conflict map too: a derived equality contradicting it stays plain.
    test_f("a = b AND b = 5 AND a != 5", "0");
    /// A non-contradicting `notEquals` seed does not block the derivation.
    test_f("a = b AND b = 5 AND a != 3", "(a = b) AND (b = 5) AND (a != 3) AND indexHint(a = 5)");

    /// Derived across sources -> stays executable (pushable below the join); the qualifier
    /// picks the source, the printed name is the bare column.
    test_f("t1.a < t2.b AND t2.b < 5", "(a < b) AND (b < 5) AND (a < 5)");
    test_f("t1.a < t1.b AND t1.b < 5", "(a < b) AND (b < 5) AND indexHint(a < 5)");
    /// Only the conjunct derived through the crossing edge itself stays executable.
    test_f(
        "t1.a < t1.b AND t1.b < t2.c AND t2.c < 5",
        "(a < b) AND (b < c) AND (c < 5) AND (b < 5) AND indexHint(a < 5)");
}
