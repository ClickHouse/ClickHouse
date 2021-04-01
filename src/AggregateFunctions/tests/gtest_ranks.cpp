#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/PODArray.h>
#include <AggregateFunctions/StatCommon.h>
#include <iostream>

#include <gtest/gtest.h>


TEST(Ranks, Simple)
{
    using namespace DB;
    RanksArray sample = {310, 195, 480, 530, 155, 530, 245, 385, 450, 450, 465, 545, 170, 180, 125, 180, 230, 170, 75, 430, 480, 495, 295};

    RanksArray ranks;
    Float64 t = 0;
    std::tie(ranks, t) = computeRanksAndTieCorrection(sample);

    RanksArray expected{12.0, 8.0, 18.5, 21.5, 3.0, 21.5, 10.0, 13.0, 15.5, 15.5, 17.0, 23.0, 4.5, 6.5, 2.0, 6.5, 9.0, 4.5, 1.0, 14.0, 18.5, 20.0, 11.0};

    ASSERT_EQ(ranks.size(), expected.size());

    for (size_t i = 0; i < ranks.size(); ++i)
        ASSERT_DOUBLE_EQ(ranks[i], expected[i]);

    ASSERT_DOUBLE_EQ(t, 0.9975296442687747);
}
