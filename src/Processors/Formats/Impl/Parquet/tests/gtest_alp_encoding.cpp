#include <gtest/gtest.h>

#include <Processors/Formats/Impl/Parquet/AlpEncoding.h>

#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <vector>

using namespace DB::Parquet::ALP;

namespace
{

template <typename T>
bool bitEqual(T a, T b)
{
    if constexpr (sizeof(T) == 8)
        return std::bit_cast<UInt64>(a) == std::bit_cast<UInt64>(b);
    else
        return std::bit_cast<UInt32>(a) == std::bit_cast<UInt32>(b);
}

template <typename T>
bool roundTrips(const std::vector<T> & in)
{
    std::vector<UInt8> page;
    Codec<T>::encodePage(in.data(), in.size(), page);
    std::vector<T> out;
    Codec<T>::decodePage(page.data(), page.size(), out);
    if (out.size() != in.size())
        return false;
    for (size_t i = 0; i < in.size(); ++i)
    {
        if (std::isnan(in[i]))
        {
            if (!std::isnan(out[i]))
                return false;
        }
        else if (!bitEqual(in[i], out[i]))
            return false;
    }
    return true;
}

}

TEST(AlpEncoding, SpecWorkedExampleByteExact)
{
    const double values[4] = {1500.0, std::numeric_limits<double>::quiet_NaN(), 2500.0, 333.5};
    std::vector<UInt8> page;
    Codec<double>::encodePage(values, 4, page, /*log_vector_size=*/10, /*forced_exponent=*/4, /*forced_factor=*/3);

    // page = 7-byte header + 4-byte offset array + 31-byte vector
    ASSERT_EQ(page.size(), 7u + 4u + 31u);
    const std::vector<UInt8> vec(page.begin() + 11, page.end());

    EXPECT_EQ(vec[0], 4);   // exponent
    EXPECT_EQ(vec[1], 3);   // factor
    EXPECT_EQ(vec[2], 1);   // num_exceptions low byte
    EXPECT_EQ(vec[3], 0);   // num_exceptions high byte

    Int64 frame_of_reference = 0;
    std::memcpy(&frame_of_reference, &vec[4], sizeof(frame_of_reference));
    EXPECT_EQ(frame_of_reference, 3335);
    EXPECT_EQ(vec[12], 15); // bit_width

    UInt16 position = 0;
    std::memcpy(&position, &vec[4 + 9 + 8], sizeof(position));
    EXPECT_EQ(position, 1);

    double exception_value = 0;
    std::memcpy(&exception_value, &vec[4 + 9 + 8 + 2], sizeof(exception_value));
    EXPECT_TRUE(std::isnan(exception_value));

    EXPECT_TRUE(roundTrips(std::vector<double>(values, values + 4)));
}

TEST(AlpEncoding, DoubleRoundTrip)
{
    std::vector<std::vector<double>> cases;

    for (size_t n : {size_t{0}, size_t{1}, size_t{1023}, size_t{1024}, size_t{1025}, size_t{3000}})
    {
        std::vector<double> v(n);
        for (size_t i = 0; i < n; ++i)
            v[i] = static_cast<double>(static_cast<Int64>(i) - static_cast<Int64>(n) / 2) / 100.0;
        cases.push_back(std::move(v));
    }

    std::vector<double> high_precision(2000);
    for (size_t i = 0; i < high_precision.size(); ++i)
        high_precision[i] = static_cast<double>(i) * 1.234567890123;
    cases.push_back(std::move(high_precision));

    std::vector<double> wide_range(2000);   // large integer-valued doubles -> big bit_width
    for (size_t i = 0; i < wide_range.size(); ++i)
        wide_range[i] = static_cast<double>(static_cast<Int64>(i) * 4000000000LL - 4000000000000LL);
    cases.push_back(std::move(wide_range));

    cases.push_back({0.0, -0.0, 1.5, -2.25, 1e18, 1e-18,
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity()});

    for (const auto & in : cases)
        EXPECT_TRUE(roundTrips(in));
}

TEST(AlpEncoding, FloatRoundTrip)
{
    std::vector<std::vector<float>> cases;

    for (size_t n : {size_t{0}, size_t{1}, size_t{1024}, size_t{2500}})
    {
        std::vector<float> v(n);
        for (size_t i = 0; i < n; ++i)
            v[i] = static_cast<float>(static_cast<Int64>(i) - static_cast<Int64>(n) / 2) / 100.0F;
        cases.push_back(std::move(v));
    }

    cases.push_back({0.0F, -0.0F, 1.5F, -2.25F,
        std::numeric_limits<float>::quiet_NaN(),
        std::numeric_limits<float>::infinity(),
        -std::numeric_limits<float>::infinity()});

    for (const auto & in : cases)
        EXPECT_TRUE(roundTrips(in));
}

TEST(AlpEncoding, EdgeCases)
{
    EXPECT_TRUE(roundTrips(std::vector<double>{}));               // empty
    EXPECT_TRUE(roundTrips(std::vector<double>(1024, 42.5)));     // all identical -> bit_width 0
    EXPECT_TRUE(roundTrips(std::vector<double>(50, std::numeric_limits<double>::quiet_NaN()))); // all exceptions

    std::vector<double> scatter(1025, 3.14);                      // exceptions across a vector boundary
    scatter[1024] = std::numeric_limits<double>::quiet_NaN();
    scatter[500] = std::numeric_limits<double>::infinity();
    scatter[1023] = -0.0;
    EXPECT_TRUE(roundTrips(scatter));
}

TEST(AlpEncoding, RejectsMalformedPages)
{
    std::vector<double> src(2500);
    for (size_t i = 0; i < src.size(); ++i)
        src[i] = static_cast<double>(i % 1000) / 10.0;
    std::vector<UInt8> good;
    Codec<double>::encodePage(src.data(), src.size(), good);

    auto rejects = [](std::vector<UInt8> page)
    {
        try
        {
            std::vector<double> out;
            Codec<double>::decodePage(page.data(), page.size(), out);
            return false;
        }
        catch (...) /* Ok: a malformed page is expected to throw */
        {
            return true;
        }
    };

    EXPECT_TRUE(rejects({good.begin(), good.begin() + 3}));                                    // truncated header
    { auto b = good; b[3] = 0xFF; b[4] = 0xFF; b[5] = 0xFF; b[6] = 0x7F; EXPECT_TRUE(rejects(b)); } // huge num_elements
    { auto b = good; b[7] = 0xFF; b[8] = 0xFF; b[9] = 0xFF; b[10] = 0x7F; EXPECT_TRUE(rejects(b)); } // offset out of range
    { auto b = good; const UInt32 off = Codec<double>::readLE32(&b[7]); const size_t vp = 7 + off; b[vp] = 100; EXPECT_TRUE(rejects(b)); }       // bad exponent
    { auto b = good; const UInt32 off = Codec<double>::readLE32(&b[7]); const size_t vp = 7 + off; b[vp + 12] = 200; EXPECT_TRUE(rejects(b)); }  // bad bit_width
    { auto b = good; const UInt32 off = Codec<double>::readLE32(&b[7]); const size_t vp = 7 + off; b[vp + 2] = 0xFF; b[vp + 3] = 0xFF; EXPECT_TRUE(rejects(b)); } // too many exceptions
}
