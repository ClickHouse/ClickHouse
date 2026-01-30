#include <gtest/gtest.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Passes/LikePerfectAffixRewritePass.h>
#include <Analyzer/Passes/tests/gtest_analyzer_utils.h>
#include <Analyzer/Utils.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

TEST(LikePerfectAffixRewrite, rewrite)
{
    tryRegisterFunctions();
    auto test_f = [&](const String & cond, const String & expected)
    {
        testPassOnCondition(
            QueryTreePassPtr(new LikePerfectAffixRewritePass()), DataTypePtr(new DataTypeString()), 
            cond, expected);
    };

    /// Perfect affix LIKE
    test_f("col LIKE 'Test%'", "startsWith(col, 'Test')");
    test_f("col LIKE 'a%'", "startsWith(col, 'a')");
    test_f("col LIKE '%Test'", "endsWith(col, 'Test')");
    test_f("col LIKE '%a'", "endsWith(col, 'a')");

    /// Perfect affix ILIKE should NOT be rewritten
    test_f("col ILIKE 'Test%'", "ilike(col, 'Test%')");

    /// Perfect affix without upper bound
    test_f("col LIKE '\xFF%'", "startsWith(col, '\xFF')");
    test_f("col LIKE '%\xFF'", "endsWith(col, '\xFF')");

    /// Perfect affix NOT LIKE
    test_f("col NOT LIKE 'Test%'", "not(startsWith(col, 'Test'))");
    test_f("col NOT LIKE '%Test'", "not(endsWith(col, 'Test'))");

    /// Imperfect affix (I)LIKE should not be rewritten
    test_f("col LIKE 'hello_world%'", "like(col, 'hello_world%')");
    test_f("col LIKE '%hello_world'", "like(col, '%hello_world')");
    test_f("col LIKE '%test%'", "like(col, '%test%')");
    test_f("col LIKE '%test_'", "like(col, '%test_')");
    test_f("col LIKE '_test%'", "like(col, '_test%')");
    test_f("col LIKE '%'", "like(col, '%')");
    test_f("col LIKE 'exactvalue'", "like(col, 'exactvalue')");

    test_f("col ILIKE 'hello_world%'", "ilike(col, 'hello_world%')");

    /// Imperfect affix NOT (I)LIKE should not be rewritten
    test_f("col NOT LIKE 'hello_world%'", "notLike(col, 'hello_world%')");
    test_f("col NOT LIKE '%hello_world'", "notLike(col, '%hello_world')");
    test_f("col NOT ILIKE 'hello_world%'", "notILike(col, 'hello_world%')");
    test_f("col NOT ILIKE '%hello_world'", "notILike(col, '%hello_world')");
}
