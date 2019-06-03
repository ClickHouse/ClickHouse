
#include <IO/BitHelpers.h>

#include <Core/Types.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBufferFromMemory.h>

#include <memory>
#include <iostream>
#include <iomanip>
#include <bitset>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-const-variable"
#pragma GCC diagnostic ignored "-Wunused-variable"

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

std::ostream & dumpBuffer(const char * begin, const char * end, std::ostream * destination, const char* col_sep = " ", const char* row_sep = "\n", const size_t cols_in_row = 8, UInt32 max_bytes = 0xFFFFFFFF)
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

std::ostream & dumpBufferContents(BufferBase & buffer, std::ostream * destination, const char* col_sep = " ", const char* row_sep = "\n", const size_t cols_in_row = 8, UInt32 max_bytes = 0xFFFFFFFF)
{
    const auto & data = buffer.buffer();
    return dumpBuffer(data.begin(), data.end(), destination, col_sep, row_sep, cols_in_row, max_bytes);
}

std::string dumpBufferContents(BufferBase & buffer, const char* col_sep = " ", const char* row_sep = "\n", const size_t cols_in_row = 8)
{
    std::stringstream sstr;
    dumpBufferContents(buffer, &sstr, col_sep, row_sep, cols_in_row);

    return sstr.str();
}


bool test(const std::vector<std::pair<UInt8, UInt64>> & bits_and_vals, const char * expected_buffer_binary = nullptr)
{
    MemoryWriteBuffer memory_write_buffer(1024, 1024, 1.5, 20*1024);

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

        if (expected_buffer_binary != nullptr)
        {
            const auto actual_buffer_binary = dumpBufferContents(*memory_read_buffer, " ", " ");
            if (actual_buffer_binary != expected_buffer_binary)
            {
                std::cerr << "Invalid buffer memory after writing\n"
                          << "expected: " << strlen(expected_buffer_binary) << "\n" << expected_buffer_binary
                          << "\ngot: " << actual_buffer_binary.size() << "\n" << actual_buffer_binary
                          << std::endl;

                return false;
            }
        }

        BitReader reader(*memory_read_buffer);

        int item = 0;
        for (const auto & bv : bits_and_vals)
        {
            const auto expected_value = getBits(bv.first, bv.second);

            const auto actual_value = reader.readBits(bv.first);

            if (expected_value != actual_value)
            {
                std::cerr << "Invalid value #" << item << " with " << static_cast<UInt32>(bv.first) << ", " << bin(bv.second) << "\n"
                        << "\texpected: " << bin(expected_value) << "\n"
                        << "\tgot     : " << bin(actual_value) << ".\n\n\nBuffer memory:\n";
                dumpBufferContents(*memory_read_buffer, &std::cerr) << std::endl << std::endl;

                return false;
            }
            ++item;
        }
    }

    return true;
}

bool primes_test()
{
    std::vector<std::pair<UInt8, UInt64>> test_data;
    MemoryWriteBuffer memory_write_buffer;

    {
        for (UInt8 r = 0; r < REPEAT_TIMES; ++r)
        {
            for (const auto p : PRIMES)
            {
                test_data.emplace_back(p, BIT_PATTERN);
            }
        }
    }

    return test(test_data);
}

void simple_test(UInt8 bits, UInt64 value)
{
    test({{bits, value}});
}

} // namespace

int main()
{
    UInt32 test_case = 0;
    for (const auto p : PRIMES)
    {
        simple_test(p, 0xFFFFFFFFFFFFFFFF);
        std::cout << ++test_case << " with all-ones and " << static_cast<UInt32>(p) << std::endl;
    }

    for (const auto p : PRIMES)
    {
        simple_test(p, BIT_PATTERN);
        std::cout << ++test_case << " with fancy bit pattern and " << static_cast<UInt32>(p) << std::endl;
    }

    test({{9, 0xFFFFFFFF}, {9, 0x00}, {9, 0xFFFFFFFF}, {9, 0x00}, {9, 0xFFFFFFFF}},
         "11111111 10000000 00111111 11100000 00001111 11111000 ");

    test({{7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {7, 0x3f}, {3, 0xFFFF}},
         "01111110 11111101 11111011 11110111 11101111 11011111 10111111 01111111 11000000 ");

    test({{33, 0xFF110d0b07050300}, {33, 0xAAEE29251f1d1713}, });
    test({{33, BIT_PATTERN}, {33, BIT_PATTERN}});

    std::cout << ++test_case << " primes " << std::endl;
    primes_test();

    return 0;
}
