#include <string.h>
#include <IO/BitHelpers.h>

#include <common/types.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/BitHelpers.h>
#include <Common/PODArray.h>

#include <cmath>
#include <memory>
#include <bitset>
#include <string>
#include <vector>
#include <typeinfo>
#include <iostream>
#pragma GCC diagnostic ignored "-Wmissing-declarations"
#include <gtest/gtest.h>

using namespace DB;

// Intentionally asymmetric both byte and word-size to detect read and write inconsistencies
// each prime bit is set to 0.
//                              v-61     v-53   v-47  v-41 v-37   v-31     v-23  v-17   v-11   v-5
const UInt64 BIT_PATTERN = 0b11101011'11101111'10111010'11101111'10101111'10111010'11101011'10101001;
const uint8_t PRIMES[] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61};

template <typename T>
std::string bin(const T & value, size_t bits = sizeof(T) * 8)
{
    static const uint8_t MAX_BITS = sizeof(T)*8;
    assert(bits <= MAX_BITS);

    return std::bitset<sizeof(T) * 8>(static_cast<uint64_t>(value))
            .to_string().substr(MAX_BITS - bits, bits);
}

// gets N low bits of value
template <typename T>
T getBits(uint8_t bits, const T & value)
{
    return value & maskLowBits<T>(bits);
}

template <typename T>
std::ostream & dumpBuffer(const T begin,
                          const T end,
                          std::ostream * destination,
                          const char* col_sep = " ",
                          const char* row_sep = "\n",
                          const size_t cols_in_row = 8,
                          UInt32 max_bytes = 0xFFFFFFFF)
{
    size_t col = 0;
    for (auto p = begin; p < end && p - begin < max_bytes; ++p)
    {
        *destination << bin(*p);
        if (++col % cols_in_row == 0)
        {
            if (row_sep)
                *destination << row_sep;
        }
        else if (col_sep)
        {
            *destination << col_sep;
        }
    }

    return *destination;
}

template <typename T>
std::string dumpContents(const T& container,
                         const char* col_sep = " ",
                         const char* row_sep = "\n",
                         const size_t cols_in_row = 8)

{
    std::stringstream sstr;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    sstr.exceptions(std::ios::failbit);
    dumpBuffer(std::begin(container), std::end(container), &sstr, col_sep, row_sep, cols_in_row);

    return sstr.str();
}

template <typename ValueLeft, typename ValueRight>
::testing::AssertionResult BinaryEqual(const ValueLeft & left, const ValueRight & right)
{
//    ::testing::AssertionResult result = ::testing::AssertionSuccess();
    if (sizeof(left) != sizeof(right))
        return ::testing::AssertionFailure()
                << "Sizes do not match, expected: " << sizeof(left) << " actual: " << sizeof(right);

    const auto size = std::min(sizeof(left), sizeof(right));
    if (memcmp(&left, &right, size) != 0)
    {
        const auto l_bits = left ? static_cast<size_t>(std::log2(left)) : 0;
        const auto r_bits = right ? static_cast<size_t>(std::log2(right)) : 0;
        const size_t bits = std::max(l_bits, r_bits) + 1;

        return ::testing::AssertionFailure()
                << "Values are binary different,\n"
                << "\texpected: 0b" << bin(left, bits) << " (" << std::hex << left << "),\n"
                << "\tactual  : 0b" << bin(right, bits) << " (" <<std::hex << right << ").";
    }

    return ::testing::AssertionSuccess();
}

struct TestCaseParameter
{
    std::vector<std::pair<uint8_t, UInt64>> bits_and_vals;
    std::string expected_buffer_binary;

    TestCaseParameter(std::vector<std::pair<uint8_t, UInt64>> vals, std::string binary = std::string{}) // NOLINT
        : bits_and_vals(std::move(vals)),
          expected_buffer_binary(binary)
    {}
};

class BitIO : public ::testing::TestWithParam<TestCaseParameter>
{};

TEST_P(BitIO, WriteAndRead)
{
    const auto & param = GetParam();
    const auto & bits_and_vals = param.bits_and_vals;
    const auto & expected_buffer_binary = param.expected_buffer_binary;

    UInt64 max_buffer_size = 0;
    for (const auto & bv : bits_and_vals)
    {
        max_buffer_size += bv.first;
    }
    max_buffer_size = (max_buffer_size + 7) / 8;
    SCOPED_TRACE(max_buffer_size);

    PODArray<char> data(max_buffer_size);

    {
        BitWriter writer(data.data(), data.size());
        for (const auto & bv : bits_and_vals)
        {
            writer.writeBits(bv.first, bv.second);
        }
        writer.flush();
    }

    {
        ReadBufferFromMemory read_buffer(data.data(), data.size());
//        auto memory_read_buffer = memory_write_buffer.tryGetReadBuffer();

        if (!expected_buffer_binary.empty())
        {
            const auto actual_buffer_binary = dumpContents(data, " ", " ");
            ASSERT_EQ(expected_buffer_binary, actual_buffer_binary);
        }

        BitReader reader(data.data(), data.size());

        int bitpos = 0;
        int item = 0;
        for (const auto & bv : bits_and_vals)
        {
            SCOPED_TRACE(::testing::Message()
                         << "item #" << item << " of " << bits_and_vals.size() << ", width: " << static_cast<UInt32>(bv.first)
                         << ", value: " << bv.second << "(" << bin(bv.second) << ")"
                         << ", at bit position: " << std::dec << reader.count()
                         << ".\nBuffer memory:\n" << dumpContents(data));

//            const uint8_t next_byte = getBits(bv.first, bv.second) &
            ASSERT_TRUE(BinaryEqual(getBits(bv.first, bv.second), reader.readBits(bv.first)));

            ++item;
            bitpos += bv.first;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(Simple,
    BitIO,
    ::testing::ValuesIn(std::initializer_list<TestCaseParameter>{
        {
            {{9, 0xFFFFFFFF}, {9, 0x00}, {9, 0xFFFFFFFF}, {9, 0x00}, {9, 0xFFFFFFFF}},
            "11111111 10000000 00111111 11100000 00001111 11111000 "
        },
        {
            {{7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {3, 0xFFFF}},
            "01111110 11111101 11111011 11110111 11101111 11011111 10111111 01111111 11000000 "
        },
        {
            {{33, 0xFF110d0b07050300}, {33, 0xAAEE29251f1d1713}}
        },
        {
            {{33, BIT_PATTERN}, {33, BIT_PATTERN}}
        },
        {
            {{24, 0xFFFFFFFF}},
            "11111111 11111111 11111111 "
        },
        {
            // Note that we take only N lower bits of the number: {3, 0b01011} => 011
            {{5, 0b01010}, {3, 0b111}, {7, 0b11001100}, {6, 0}, {5, 0b11111111}, {4, 0}, {3, 0b101}, {2, 0}, {1, 0b11111111}},
            "01010111 10011000 00000111 11000010 10010000 "
        },
        {
            {{64, BIT_PATTERN}, {56, BIT_PATTERN} , {4, 0b1111}, {4, 0}, // 128
             {8, 0b11111111}, {64, BIT_PATTERN}, {48, BIT_PATTERN}, {8, 0}}, // 256
            "11101011 11101111 10111010 11101111 10101111 10111010 11101011 10101001 " // 64
            "11101111 10111010 11101111 10101111 10111010 11101011 10101001 11110000 " // 128
            "11111111 11101011 11101111 10111010 11101111 10101111 10111010 11101011 " // 192
            "10101001 10111010 11101111 10101111 10111010 11101011 10101001 00000000 " // 256
        },
        {
            {{64, BIT_PATTERN}, {56, BIT_PATTERN} , {5, 0b11111}, {3, 0}, // 128
             {8, 0b11111111}, {64, BIT_PATTERN}, {48, BIT_PATTERN}, {8, 0}, //256
             {32, BIT_PATTERN}, {12, 0xff}, {8, 0}, {12, 0xAEff}},
            "11101011 11101111 10111010 11101111 10101111 10111010 11101011 10101001 " // 64
            "11101111 10111010 11101111 10101111 10111010 11101011 10101001 11111000 " // 128
            "11111111 11101011 11101111 10111010 11101111 10101111 10111010 11101011 " // 192
            "10101001 10111010 11101111 10101111 10111010 11101011 10101001 00000000 " // 256
            "10101111 10111010 11101011 10101001 00001111 11110000 00001110 11111111 " // 320
        }
    })
);

TestCaseParameter primes_case(uint8_t repeat_times, UInt64 pattern)
{
    std::vector<std::pair<uint8_t, UInt64>> test_data;

    {
        for (uint8_t r = 0; r < repeat_times; ++r)
        {
            for (const auto p : PRIMES)
            {
                test_data.emplace_back(p, pattern);
            }
        }
    }

    return TestCaseParameter(test_data);
}

INSTANTIATE_TEST_SUITE_P(Primes,
    BitIO,
    ::testing::Values(
        primes_case(11, 0xFFFFFFFFFFFFFFFFULL),
        primes_case(11, BIT_PATTERN)
    )
);

TEST(BitHelpers, maskLowBits)
{
    EXPECT_EQ(0b00000111, ::maskLowBits<uint8_t>(3));
    EXPECT_EQ(0b01111111, ::maskLowBits<uint8_t>(7));
    EXPECT_EQ(0b0000000001111111, ::maskLowBits<UInt16>(7));
    EXPECT_EQ(0b0001111111111111, ::maskLowBits<UInt16>(13));
    EXPECT_EQ(0b00000111111111111111111111111111, ::maskLowBits<UInt32>(27));
    EXPECT_EQ(0b111111111111111111111111111111111, ::maskLowBits<UInt64>(33));
    EXPECT_EQ(0b11111111111111111111111111111111111, ::maskLowBits<UInt64>(35));

    EXPECT_EQ(0xFF, ::maskLowBits<uint8_t>(8));
    EXPECT_EQ(0xFFFF, ::maskLowBits<UInt16>(16));
    EXPECT_EQ(0xFFFFFFFF, ::maskLowBits<UInt32>(32));
    EXPECT_EQ(0xFFFFFFFFFFFFFFFF, ::maskLowBits<UInt64>(64));
}
