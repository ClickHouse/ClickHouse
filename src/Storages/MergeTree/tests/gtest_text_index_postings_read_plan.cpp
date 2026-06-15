#include <Storages/MergeTree/TextIndexAnalyzer.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

TokenPostingsInfoPtr makeTokenInfo(UInt32 cardinality, std::initializer_list<RowsRange> ranges)
{
    auto info = std::make_shared<TokenPostingsInfo>();
    info->cardinality = cardinality;

    size_t offset = 0;
    for (const auto & range : ranges)
    {
        info->ranges.push_back(range);
        info->offsets.push_back(offset++);
    }

    return info;
}

TextIndexAnalyzer::QueryBuilder makeAllQueryBuilder()
{
    VectorWithMemoryTracking<String> tokens;
    tokens.emplace_back("dense");
    tokens.emplace_back("empty");
    tokens.emplace_back("expensive_rare");
    tokens.emplace_back("rare");

    TextIndexAnalyzer::QueryBuilder query_builder;
    query_builder.query = std::make_shared<TextSearchQuery>(
        "hasAllTokens", TextSearchMode::All, TextIndexDirectReadMode::None, std::move(tokens));

    query_builder.tokens.emplace("dense", makeTokenInfo(1000, {RowsRange{100, 199}}));
    query_builder.tokens.emplace("empty", makeTokenInfo(1000, {RowsRange{0, 50}, RowsRange{250, 300}}));
    query_builder.tokens.emplace("expensive_rare", makeTokenInfo(5, {RowsRange{80, 120}, RowsRange{130, 160}}));
    query_builder.tokens.emplace("rare", makeTokenInfo(10, {RowsRange{100, 199}}));

    return query_builder;
}

}

TEST(TextIndexPostingsReadPlan, AllQueryPrefersEmptyThenCheapestAndRarestToken)
{
    const auto query_builder = makeAllQueryBuilder();
    const RowsRange current_range{100, 199};
    const absl::flat_hash_set<String> tokens_with_postings;

    const auto plan = TextIndexAnalyzer::buildPostingsReadPlan(query_builder, current_range, tokens_with_postings);

    ASSERT_EQ(plan.size(), 4);
    EXPECT_EQ(plan[0].token, "empty");
    EXPECT_TRUE(plan[0].blocks_to_read.empty());
    EXPECT_EQ(plan[1].token, "rare");
    EXPECT_EQ(plan[2].token, "dense");
    EXPECT_EQ(plan[3].token, "expensive_rare");
}

TEST(TextIndexPostingsReadPlan, SkipsAlreadyReadPostings)
{
    const auto query_builder = makeAllQueryBuilder();
    const RowsRange current_range{100, 199};
    const absl::flat_hash_set<String> tokens_with_postings{"rare"};

    const auto plan = TextIndexAnalyzer::buildPostingsReadPlan(query_builder, current_range, tokens_with_postings);

    ASSERT_EQ(plan.size(), 3);
    EXPECT_EQ(plan[0].token, "empty");
    EXPECT_EQ(plan[1].token, "dense");
    EXPECT_EQ(plan[2].token, "expensive_rare");
}
