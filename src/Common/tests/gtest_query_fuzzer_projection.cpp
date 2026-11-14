#include <gtest/gtest.h>

#include <Common/QueryFuzzer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

TEST(QueryFuzzer, ProjectionFormSwitchClearsCodecColumns)
{
    const String sql =
        "CREATE TABLE t_projection_fuzzer (k UInt64, PROJECTION p (k CODEC(ZSTD)) AS (SELECT k ORDER BY k)) "
        "ENGINE = MergeTree ORDER BY k";
    ParserQuery parser(sql.data() + sql.size());
    ASTPtr base = parseQuery(parser, sql.data(), sql.data() + sql.size(), "", 0, 0, 0);

    size_t switched_to_index = 0;
    for (UInt64 seed = 0; seed < 300; ++seed)
    {
        ASTPtr ast = base->clone();
        QueryFuzzer fuzzer{pcg64(seed)};
        try
        {
            fuzzer.fuzzMain(ast);
        }
        catch (...)
        {
            /// Other random mutations can throw; this test checks the completed form switches.
            continue;
        }

        const auto * create = ast->as<ASTCreateQuery>();
        if (!create || !create->columns_list || !create->columns_list->projections)
            continue;

        for (const auto & child : create->columns_list->projections->children)
        {
            const auto * projection = child->as<ASTProjectionDeclaration>();
            if (!projection || !projection->index)
                continue;

            ++switched_to_index;
            ASSERT_EQ(projection->columns, nullptr) << "seed=" << seed;
            EXPECT_NO_THROW(projection->clone()) << "seed=" << seed;
        }
    }
    EXPECT_GT(switched_to_index, 0u);
}
