#include <string>
#include <vector>

#include <IO/WriteBufferFromVector.h>
#include <Common/FST.h>
#include <gtest/gtest.h>

TEST(FST, SimpleTest)
{
    std::vector<std::pair<String, DB::FST::Output>> indexed_data
    {
        {"mop", 100},
        {"moth", 91},
        {"pop", 72},
        {"star", 83},
        {"stop", 54},
        {"top", 55},
    };

    std::vector<std::pair<String, DB::FST::Output>> not_indexed_data
    {
        {"mo", 100},
        {"moth1", 91},
        {"po", 72},
        {"star2", 83},
        {"sto", 54},
        {"top33", 55},
    };

    std::vector<UInt8> buffer;
    {
        DB::WriteBufferFromVector<std::vector<UInt8>> wbuf(buffer);
        DB::FST::FstBuilder builder(wbuf);

        for (auto & [term, output] : indexed_data)
            builder.add(term, output);
        builder.build();
    }

    DB::FST::FiniteStateTransducer fst(buffer);
    for (auto & [term, output] : indexed_data)
    {
        auto [result, found] = fst.getOutput(term);
        ASSERT_TRUE(found);
        ASSERT_EQ(result, output);
    }

    for (auto & [term, output] : not_indexed_data)
    {
        auto [result, found] = fst.getOutput(term);
        ASSERT_FALSE(found);
    }
}

TEST(FST, TestForLongTerms)
{
    /// Test long terms within limitation
    String term1(DB::FST::MAX_TERM_LENGTH - 1, 'A');
    String term2(DB::FST::MAX_TERM_LENGTH, 'B');

    DB::FST::Output output1 = 100;
    DB::FST::Output output2 = 200;

    std::vector<UInt8> buffer;
    {
        DB::WriteBufferFromVector<std::vector<UInt8>> wbuf(buffer);
        DB::FST::FstBuilder builder(wbuf);

        builder.add(term1, output1);
        builder.add(term2, output2);

        builder.build();
    }

    DB::FST::FiniteStateTransducer fst(buffer);

    auto [result1, found1] = fst.getOutput(term1);
    ASSERT_TRUE(found1);
    ASSERT_EQ(result1, output1);

    auto [result2, found2] = fst.getOutput(term2);
    ASSERT_TRUE(found2);
    ASSERT_EQ(result2, output2);

    /// Test exception case when term length exceeds limitation
    String term3(DB::FST::MAX_TERM_LENGTH + 1, 'C');
    DB::FST::Output output3 = 300;

    std::vector<UInt8> buffer3;
    DB::WriteBufferFromVector<std::vector<UInt8>> wbuf3(buffer3);
    DB::FST::FstBuilder builder3(wbuf3);

    EXPECT_THROW(builder3.add(term3, output3), DB::Exception);
}
