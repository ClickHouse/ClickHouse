#include "BitHelpers.h"

#include <cassert>

namespace
{
const DB::UInt8 MAX_BUFFER_SIZE_BITS = 8;
}

namespace DB
{

BitReader::BitReader(ReadBuffer & buf_)
    : buf(buf_),
      bits_buffer(0),
      bits_count(0)
{}

BitReader::~BitReader()
{}

UInt64 BitReader::readBits(UInt8 bits)
{
    UInt64 result = 0;
    bits = std::min(static_cast<UInt8>(sizeof(result) * 8), bits);

    while (bits != 0)
    {
        if (bits_count == 0)
        {
            fillBuffer();
            if (bits_count == 0)
            {
                // EOF.
                break;
            }
        }

        const auto to_read = std::min(bits, bits_count);
        // read MSB bits from bits_bufer
        const UInt8 v = bits_buffer >> (bits_count - to_read);
        const UInt8 mask = static_cast<UInt8>(~(~0U << to_read));
        const UInt8 value = v & mask;
        result |= value;

        // unset MSB that were read
        bits_buffer &= ~(mask << (bits_count - to_read));
        bits_count -= to_read;
        bits -= to_read;

        result <<= std::min(bits, static_cast<UInt8>(sizeof(bits_buffer)*8));
    }

    return result;
}

UInt8 BitReader::readBit()
{
    return static_cast<UInt8>(readBits(1));
}

bool BitReader::eof() const
{
    return bits_count == 0 && buf.eof();
}

void BitReader::fillBuffer()
{
    auto read = buf.read(reinterpret_cast<char *>(&bits_buffer), MAX_BUFFER_SIZE_BITS/8);
    bits_count = static_cast<UInt8>(read) * 8;
}

BitWriter::BitWriter(WriteBuffer & buf_)
    : buf(buf_),
      bits_buffer(0),
      bits_count(0)
{}

BitWriter::~BitWriter()
{
    flush();
}

void BitWriter::writeBits(UInt8 bits, UInt64 value)
{
    bits = std::min(static_cast<UInt8>(sizeof(value) * 8), bits);

    while (bits > 0)
    {
        auto v = value;
        auto to_write = bits;

        const UInt8 capacity = MAX_BUFFER_SIZE_BITS - bits_count;
        if (capacity < bits)
        {
            // write MSB:
            v >>= bits - capacity;
            to_write = capacity;
        }


        const UInt64 mask = (1 << to_write) - 1;
        v &= mask;
        assert(v <= 255);

        bits_buffer <<= to_write;
        bits_buffer |= v;
        bits_count += to_write;

        if (bits_count < MAX_BUFFER_SIZE_BITS)
            break;

        doFlush();
        bits -= to_write;
    }
}

void BitWriter::flush()
{
    if (bits_count != 0)
    {
        bits_buffer <<= (MAX_BUFFER_SIZE_BITS - bits_count);
        doFlush();
    }
}

void BitWriter::doFlush()
{
    buf.write(reinterpret_cast<const char *>(&bits_buffer), MAX_BUFFER_SIZE_BITS/8);

    bits_count = 0;
    bits_buffer = 0;
}

} // namespace DB
