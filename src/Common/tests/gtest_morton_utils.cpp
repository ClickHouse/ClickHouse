#include <gtest/gtest.h>
#include <iostream>
#include <Common/MortonUtils.h>


GTEST_TEST(MortonUtils, Intervals)
{
    {
        std::stringstream res;
        intervalBinaryPartition(6, 13, [&](UInt64 first, UInt64 last)
        {
            res << first << ", " << last << "; ";
        });
        ASSERT_EQ(res.str(), "6, 7; 8, 11; 12, 13; ");
    }

    {
        std::stringstream res;
        intervalBinaryPartition(15, 31, [&](UInt64 first, UInt64 last)
        {
            res << first << ", " << last << "; ";
        });
        ASSERT_EQ(res.str(), "15, 15; 16, 31; ");
    }

    {
        std::stringstream res;
        intervalBinaryPartition(15, 16, [&](UInt64 first, UInt64 last)
        {
            res << first << ", " << last << "; ";
        });
        ASSERT_EQ(res.str(), "15, 15; 16, 16; ");
    }

    {
        std::stringstream res;
        intervalBinaryPartition(191, 769, [&](UInt64 first, UInt64 last)
        {
            res << first << ", " << last << "; ";
        });
        ASSERT_EQ(res.str(), "191, 191; 192, 255; 256, 511; 512, 767; 768, 769; ");
    }

    {
        std::array<std::pair<UInt64, UInt64>, 2> input = {std::pair{6, 13}, std::pair{15, 31}};

        std::stringstream res;
        hyperrectangleBinaryPartition<2>(input, [&](auto hyperrectangle)
        {
            res << "[" << hyperrectangle[0].first << ", " << hyperrectangle[0].second
                << "] x [" << hyperrectangle[1].first << ", " << hyperrectangle[1].second
                << "]; ";
        });

        ASSERT_EQ(res.str(), "[6, 7] x [15, 15]; [6, 7] x [16, 31]; [8, 11] x [15, 15]; [8, 11] x [16, 31]; [12, 13] x [15, 15]; [12, 13] x [16, 31]; ");
    }

    {
        std::array<std::pair<UInt64, UInt64>, 2> input = {std::pair{23, 24}, std::pair{15, 16}};

        std::stringstream res;
        hyperrectangleBinaryPartition<2>(input, [&](auto hyperrectangle)
        {
            res << "[" << hyperrectangle[0].first << ", " << hyperrectangle[0].second
                << "] x [" << hyperrectangle[1].first << ", " << hyperrectangle[1].second
                << "]; ";
        });

        ASSERT_EQ(res.str(), "[23, 23] x [15, 15]; [23, 23] x [16, 16]; [24, 24] x [15, 15]; [24, 24] x [16, 16]; ");
    }

    {
        std::stringstream res;
        mortonIntervalToHyperrectangles<2>(191, 769, [&](auto hyperrectangle)
        {
            res << "[" << hyperrectangle[0].first << ", " << hyperrectangle[0].second
                << "] x [" << hyperrectangle[1].first << ", " << hyperrectangle[1].second
                << "]; ";
        });

        ASSERT_EQ(res.str(), "[7, 7] x [15, 15]; [8, 15] x [8, 15]; [16, 31] x [0, 15]; [0, 15] x [16, 31]; [16, 17] x [16, 16]; ");
    }

    {
        std::stringstream res;
        mortonIntervalToHyperrectangles<2>(500, 600, [&](auto hyperrectangle)
        {
            res << "[" << hyperrectangle[0].first << ", " << hyperrectangle[0].second
                << "] x [" << hyperrectangle[1].first << ", " << hyperrectangle[1].second
                << "]; ";
        });

        ASSERT_EQ(res.str(), "[30, 31] x [12, 13]; [28, 31] x [14, 15]; [0, 7] x [16, 23]; [8, 11] x [16, 19]; [12, 15] x [16, 17]; [12, 12] x [18, 18]; ");
    }

    {
        std::array<std::pair<UInt64, UInt64>, 2> input = {std::pair{23, 24}, std::pair{15, 16}};

        std::stringstream res;
        hyperrectangleToPossibleMortonIntervals<2>(input, [&](UInt64 first, UInt64 last)
        {
            res << first << ", " << last << "; ";
        });

        ASSERT_EQ(res.str(), "447, 447; 789, 789; 490, 490; 832, 832; ");
    }

    {
        std::array<std::pair<UInt64, UInt64>, 2> input = {std::pair{6, 7}, std::pair{16, 31}};

        std::stringstream res;
        hyperrectangleToPossibleMortonIntervals<2>(input, [&](UInt64 first, UInt64 last)
        {
            res << first << ", " << last << "; ";
        });

        ASSERT_EQ(res.str(), "512, 767; ");
    }

    {
        std::array<std::pair<UInt64, UInt64>, 2> input = {std::pair{6, 13}, std::pair{15, 31}};

        std::stringstream res;
        hyperrectangleToPossibleMortonIntervals<2>(input, [&](UInt64 first, UInt64 last)
        {
            res << first << ", " << last << "; ";
        });

        ASSERT_EQ(res.str(), "188, 191; 512, 767; 224, 239; 248, 251; ");
    }
}
