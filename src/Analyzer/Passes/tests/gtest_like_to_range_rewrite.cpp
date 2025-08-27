#include <gtest/gtest.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Passes/LikeToRangeRewritePass.h>
#include <Analyzer/Passes/tests/gtest_analyzer_utils.h>
#include <Analyzer/Utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

TEST(LikeToRangeRewrite, rewrite)
{
    tryRegisterFunctions();
    auto test_f = [&](const String & cond, const String & expected)
    {
        testPassOnCondition(
            QueryTreePassPtr(new LikeToRangeRewritePass()), DataTypePtr(new DataTypeString()), 
            cond, expected);
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
