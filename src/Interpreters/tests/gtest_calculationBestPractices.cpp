#include <gtest/gtest.h>

#include <Interpreters/BloomFilterHash.h>

using namespace DB;


TEST(BloomFilterHash, ReasonableProbabilities)
{
    const auto & output_1 = BloomFilterHash::calculationBestPractices(0.001);

    ASSERT_EQ(output_1.first, 15);
    ASSERT_EQ(output_1.second, 7);

    const auto & output_2 = BloomFilterHash::calculationBestPractices(0.025);

    ASSERT_EQ(output_2.first, 8);
    ASSERT_EQ(output_2.second, 4);

    const auto & output_3 = BloomFilterHash::calculationBestPractices(0.05);

    ASSERT_EQ(output_3.first, 7);
    ASSERT_EQ(output_3.second, 3);

    const auto & output_4 = BloomFilterHash::calculationBestPractices(0.1);

    ASSERT_EQ(output_4.first, 5);
    ASSERT_EQ(output_4.second, 3);

    const auto & output_5 = BloomFilterHash::calculationBestPractices(0.2);

    ASSERT_EQ(output_5.first, 4);
    ASSERT_EQ(output_5.second, 2);

    const auto & output_6 = BloomFilterHash::calculationBestPractices(0.282);

    ASSERT_EQ(output_6.first, 3);
    ASSERT_EQ(output_6.second, 2);
}

TEST(BloomFilterHash, HighProbabilities)
{
    const auto & output_1 = BloomFilterHash::calculationBestPractices(0.283);

    ASSERT_EQ(output_1.first, 2);
    ASSERT_EQ(output_1.second, 2);

    const auto & output_2 = BloomFilterHash::calculationBestPractices(0.5);

    ASSERT_EQ(output_2.first, 2);
    ASSERT_EQ(output_2.second, 2);

    const auto & output_3 = BloomFilterHash::calculationBestPractices(0.8);

    ASSERT_EQ(output_3.first, 2);
    ASSERT_EQ(output_3.second, 2);
}
