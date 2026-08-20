#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <vector>
#include <Processors/Formats/Impl/Parquet/AlpEncoding.h>
#include <gtest/gtest.h>

using namespace DB::Parquet::ALP;
template <typename T>
static bool bitEq(T a, T b)
{
    return std::memcmp(&a, &b, sizeof(T)) == 0;
}
template <typename T>
static bool roundTrips(const std::vector<T> & in)
{
    std::vector<UInt8> page;
    Codec<T>::encodePage(in.data(), in.size(), page);
    std::vector<T> out;
    Codec<T>::decodePage(page.data(), out);
    if (out.size() != in.size())
        return false;
    for (size_t i = 0; i < in.size(); ++i)
        if (std::isnan(in[i]))
        {
            if (!std::isnan(out[i]))
                return false;
        }
        else if (!bitEq(in[i], out[i]))
            return false;
    return true;
}

TEST(AlpEncoding, SpecWorkedExampleByteExact)
{
    double vals[4] = {1500.0, std::nan(""), 2500.0, 333.5};
    std::vector<UInt8> page;
    Codec<double>::encodePage(vals, 4, page, /*logVec=*/10, /*e=*/4, /*f=*/3);
    std::vector<UInt8> vec(page.begin() + 11, page.end()); // 7 header + 4 offset
    ASSERT_EQ(vec.size(), 31u); // spec's 31-byte vector
    EXPECT_EQ(vec[0], 4);
    EXPECT_EQ(vec[1], 3); // exponent=4, factor=3
    EXPECT_EQ(vec[2], 1);
    EXPECT_EQ(vec[3], 0); // num_exceptions=1
    int64_t forRef;
    std::memcpy(&forRef, &vec[4], 8);
    EXPECT_EQ(forRef, 3335); // frame_of_reference
    EXPECT_EQ(vec[12], 15); // bit_width
    uint16_t pos;
    std::memcpy(&pos, &vec[4 + 9 + 8], 2);
    EXPECT_EQ(pos, 1); // exception position
    double exc;
    std::memcpy(&exc, &vec[4 + 9 + 8 + 2], 8);
    EXPECT_TRUE(std::isnan(exc)); // exception value NaN preserved
}

TEST(AlpEncoding, DoubleRoundTrip)
{
    std::mt19937_64 rng(12345);
    for (int t = 0; t < 200; ++t)
    {
        size_t n = rng() % 4000;
        std::vector<double> in(n);
        std::uniform_int_distribution<int> k(0, 5);
        for (auto & x : in)
            switch (k(rng))
            {
                case 0: x = static_cast<double>(static_cast<int64_t>(rng() % 2000000) - 1000000) / 100.0; break;
                case 1: x = static_cast<double>(static_cast<int64_t>(rng() % 1000000)) / 1000.0; break;
                case 2: {
                    uint64_t b = rng();
                    std::memcpy(&x, &b, 8);
                    break;
                }
                case 3: x = 0.0; break;
                case 4: x = std::nan(""); break;
                case 5: x = (rng() & 1) ? std::numeric_limits<double>::infinity() : -0.0; break;
            }
        ASSERT_TRUE(roundTrips(in)) << "n=" << n << " trial=" << t;
    }
}

TEST(AlpEncoding, FloatRoundTrip)
{
    std::mt19937 rng(999);
    for (int t = 0; t < 200; ++t)
    {
        size_t n = rng() % 4000;
        std::vector<float> in(n);
        std::uniform_int_distribution<int> k(0, 4);
        for (auto & x : in)
            switch (k(rng))
            {
                case 0: x = static_cast<float>(static_cast<int>(rng() % 200000) - 100000) / 100.0f; break;
                case 1: {
                    uint32_t b = rng();
                    std::memcpy(&x, &b, 4);
                    break;
                }
                case 2: x = 0.0f; break;
                case 3: x = std::nanf(""); break;
                case 4: x = (rng() & 1) ? static_cast<float>(INFINITY) : -0.0f; break;
            }
        ASSERT_TRUE(roundTrips(in)) << "n=" << n << " trial=" << t;
    }
}

TEST(AlpEncoding, EdgeCases)
{
    EXPECT_TRUE(roundTrips(std::vector<double>{})); // empty
    EXPECT_TRUE(roundTrips(std::vector<double>(1024, 42.5))); // all identical -> bit_width 0
    EXPECT_TRUE(roundTrips(std::vector<double>(50, std::nan("")))); // all exceptions
    std::vector<double> b(1025, 3.14);
    b[1024] = std::nan("");
    b[500] = std::numeric_limits<double>::infinity();
    b[1023] = -0.0;
    EXPECT_TRUE(roundTrips(b)); // scatter across vector boundary
    EXPECT_TRUE(roundTrips(std::vector<double>{9.22e18, -9.22e18, 0.0, 1.0, -1.0, 1e-18, 1e18})); // near int64 limits
    std::vector<double> w(2000);
    std::mt19937_64 r(7);
    for (auto & x : w)
        x = static_cast<double>(static_cast<int64_t>(r()) >> 10);
    EXPECT_TRUE(roundTrips(w)); // wide range -> bit_width ~64
}
