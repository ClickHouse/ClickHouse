#include <IO/BitHelpers.h>

#include <Core/Types.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromMemory.h>

#include <gtest/gtest.h>

#include <memory>
#include <iostream>
#include <iomanip>
#include <bitset>

//#pragma GCC diagnostic push
//#pragma GCC diagnostic ignored "-Wunused-const-variable"
//#pragma GCC diagnostic ignored "-Wunused-variable"
//#pragma GCC diagnostic ignored "-Wunused-function"

namespace
{
using namespace DB;

// Intentionally asymetric both byte and word-size to detect read and write inconsistencies
// each prime bit is set to 0.
//                              v-61     v-53   v-47  v-41 v-37   v-31     v-23  v-17   v-11   v-5
const UInt64 BIT_PATTERN = 0b11101011'11101111'10111010'11101111'10101111'10111010'11101011'10101001;
const UInt8 PRIMES[] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61};
const UInt8 REPEAT_TIMES = 11;

template <typename T>
std::string bin(const T & value, size_t bits = sizeof(T)*8)
{
    static const UInt8 MAX_BITS = sizeof(T)*8;
    assert(bits <= MAX_BITS);

    return std::bitset<sizeof(T) * 8>(static_cast<unsigned long long>(value))
            .to_string().substr(MAX_BITS - bits, bits);
}

template <typename T>
T getBits(UInt8 bits, const T & value)
{
    const T mask = ((static_cast<T>(1) << static_cast<T>(bits)) - 1);
    return value & mask;
}

std::ostream & dumpBuffer(const char * begin,
                          const char * end,
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

std::string dumpBufferContents(BufferBase & buf,
                               const char* col_sep = " ",
                               const char* row_sep = "\n",
                               const size_t cols_in_row = 8)

{
    std::stringstream sstr;
    dumpBuffer(buf.buffer().begin(), buf.buffer().end(), &sstr, col_sep, row_sep, cols_in_row);

    return sstr.str();
}

struct TestCaseParameter
{
    std::vector<std::pair<UInt8, UInt64>> bits_and_vals;
    std::string expected_buffer_binary;

    explicit TestCaseParameter(std::vector<std::pair<UInt8, UInt64>> vals, std::string binary = std::string{})
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
    max_buffer_size = (max_buffer_size + 8) / 8;
    SCOPED_TRACE(max_buffer_size);

    MemoryWriteBuffer memory_write_buffer(max_buffer_size * 2, max_buffer_size, 1.5, max_buffer_size);

    {
        BitWriter writer(memory_write_buffer);
        for (const auto & bv : bits_and_vals)
        {
            writer.writeBits(bv.first, bv.second);
        }
        writer.flush();
    }

    {
        auto memory_read_buffer = memory_write_buffer.tryGetReadBuffer();

        if (expected_buffer_binary != std::string{})
        {
            const auto actual_buffer_binary = dumpBufferContents(*memory_read_buffer, " ", " ");
            ASSERT_EQ(expected_buffer_binary, actual_buffer_binary);
        }

        BitReader reader(*memory_read_buffer);

        int item = 0;
        for (const auto & bv : bits_and_vals)
        {
            const auto actual_value = reader.readBits(bv.first);

            ASSERT_EQ(getBits(bv.first, bv.second), actual_value)
                    << "item #" << item << ", width: " << static_cast<UInt32>(bv.first)
                    << ", value: " << bin(bv.second)
                    << ".\n\n\nBuffer memory:\n" << dumpBufferContents(*memory_read_buffer);

            ++item;
        }
    }
}

INSTANTIATE_TEST_CASE_P(Simple,
        BitIO,
        ::testing::Values(
            TestCaseParameter(
                {{9, 0xFFFFFFFF}, {9, 0x00}, {9, 0xFFFFFFFF}, {9, 0x00}, {9, 0xFFFFFFFF}},
                "11111111 10000000 00111111 11100000 00001111 11111000 "),
            TestCaseParameter(
                {{7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {3, 0xFFFF}},
                "01111110 11111101 11111011 11110111 11101111 11011111 10111111 01111111 11000000 "),
            TestCaseParameter({{33, 0xFF110d0b07050300}, {33, 0xAAEE29251f1d1713}}),
            TestCaseParameter({{33, BIT_PATTERN}, {33, BIT_PATTERN}})
));

TestCaseParameter primes_case(UInt8 repeat_times, UInt64 pattern)
{
    std::vector<std::pair<UInt8, UInt64>> test_data;

    {
        for (UInt8 r = 0; r < repeat_times; ++r)
        {
            for (const auto p : PRIMES)
            {
                test_data.emplace_back(p, pattern);
            }
        }
    }

    return TestCaseParameter(test_data);
}

INSTANTIATE_TEST_CASE_P(Primes,
        BitIO,
        ::testing::Values(
            primes_case(11, 0xFFFFFFFFFFFFFFFFULL),
            primes_case(11, BIT_PATTERN)
));

} // namespace

