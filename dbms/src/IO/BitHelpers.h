#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Types.h>
#include <Common/BitHelpers.h>

#if defined(__OpenBSD__) || defined(__FreeBSD__)
#   include <sys/endian.h>
#elif defined(__APPLE__)
#   include <libkern/OSByteOrder.h>

#   define htobe64(x) OSSwapHostToBigInt64(x)
#   define be64toh(x) OSSwapBigToHostInt64(x)
#endif

namespace DB
{

/** Reads data from underlying ReadBuffer bit by bit, max 64 bits at once.
 *
 * reads MSB bits first, imagine that you have a data:
 * 11110000 10101010 00100100 11111110
 *
 * Given that r is BitReader created with a ReadBuffer that reads from data above:
 *  r.readBits(3)  => 0b111
 *  r.readBit()    => 0b1
 *  r.readBits(8)  => 0b1010 // 4 leading zero-bits are not shown
 *  r.readBit()    => 0b1
 *  r.readBit()    => 0b0
 *  r.readBits(15) => 0b10001001001111111
 *  r.readBit()    => 0b0
**/

class BitReader
{
    ReadBuffer & buf;

    UInt64 bits_buffer;
    UInt8 bits_count;
    static constexpr UInt8 BIT_BUFFER_SIZE = sizeof(bits_buffer) * 8;

public:
    BitReader(ReadBuffer & buf_)
        : buf(buf_),
          bits_buffer(0),
          bits_count(0)
    {}

    ~BitReader()
    {}

    inline UInt64 readBits(UInt8 bits)
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

            const UInt64 v = bits_buffer >> (bits_count - to_read);
            const UInt64 mask = maskLowBits<UInt64>(to_read);
            const UInt64 value = v & mask;
            result |= value;

            // unset bits that were read
            bits_buffer &= ~(mask << (bits_count - to_read));
            bits_count -= to_read;
            bits -= to_read;

            result <<= std::min(bits, BIT_BUFFER_SIZE);
        }

        return result;
    }

    inline UInt64 peekBits(UInt8 /*bits*/)
    {
        return 0;
    }

    inline UInt8 readBit()
    {
        return static_cast<UInt8>(readBits(1));
    }

    inline bool eof() const
    {
        return bits_count == 0 && buf.eof();
    }

private:
    void fillBuffer()
    {
        auto read = buf.read(reinterpret_cast<char *>(&bits_buffer), BIT_BUFFER_SIZE / 8);
        bits_buffer = be64toh(bits_buffer);
        bits_buffer >>= BIT_BUFFER_SIZE - read * 8;

        bits_count = static_cast<UInt8>(read) * 8;
    }
};

class BitWriter
{
    WriteBuffer & buf;

    UInt64 bits_buffer;
    UInt8 bits_count;

    static constexpr UInt8 BIT_BUFFER_SIZE = sizeof(bits_buffer) * 8;

public:
    BitWriter(WriteBuffer & buf_)
        : buf(buf_),
          bits_buffer(0),
          bits_count(0)
    {}

    ~BitWriter()
    {
        flush();
    }

    inline void writeBits(UInt8 bits, UInt64 value)
    {
        bits = std::min(static_cast<UInt8>(sizeof(value) * 8), bits);

        while (bits > 0)
        {
            auto v = value;
            auto to_write = bits;

            const UInt8 capacity = BIT_BUFFER_SIZE - bits_count;
            if (capacity < bits)
            {
                v >>= bits - capacity;
                to_write = capacity;
            }

            const UInt64 mask = maskLowBits<UInt64>(to_write);
            v &= mask;

            bits_buffer <<= to_write;
            bits_buffer |= v;
            bits_count += to_write;

            if (bits_count < BIT_BUFFER_SIZE)
                break;

            doFlush();
            bits -= to_write;
        }
    }

    inline void flush()
    {
        if (bits_count != 0)
        {
            bits_buffer <<= (BIT_BUFFER_SIZE - bits_count);
            doFlush();
        }
    }

private:
    void doFlush()
    {
        bits_buffer = htobe64(bits_buffer);
        buf.write(reinterpret_cast<const char *>(&bits_buffer), (bits_count + 7) / 8);

        bits_count = 0;
        bits_buffer = 0;
    }
};

}
