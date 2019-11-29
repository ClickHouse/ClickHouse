#pragma once

#include <Core/Types.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>

#include <string.h>

#if defined(__OpenBSD__) || defined(__FreeBSD__)
#   include <sys/endian.h>
#elif defined(__APPLE__)
#   include <libkern/OSByteOrder.h>

#   define htobe64(x) OSSwapHostToBigInt64(x)
#   define be64toh(x) OSSwapBigToHostInt64(x)
#endif


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
extern const int ATTEMPT_TO_READ_AFTER_EOF;
}

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
    using BufferType = UInt64;

    const char * source_begin;
    const char * source_current;
    const char * source_end;

    BufferType bits_buffer;
    UInt8 bits_count;
    static constexpr UInt8 BIT_BUFFER_SIZE = sizeof(bits_buffer) * 8;

public:
    BitReader(const char * begin, size_t size)
        : source_begin(begin),
          source_current(begin),
          source_end(begin + size),
          bits_buffer(0),
          bits_count(0)
    {}

    ~BitReader()
    {}

    inline UInt64 readBits(UInt8 bits)
    {
        UInt64 result = 0;

        while (bits != 0)
        {
            if (bits_count == 0)
            {
                if (fillBuffer() == 0)
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

    inline UInt8 peekByte() const
    {
        UInt8 result = 0;
        const UInt64 v = bits_buffer >> (bits_count - 8);
        const UInt64 mask = maskLowBits<UInt64>(8);
        const UInt64 value = v & mask;
        result |= value;

        return result;
    }

    inline UInt8 readBit()
    {
        return static_cast<UInt8>(readBits(1));
    }

    inline bool eof() const
    {
        return bits_count == 0 && source_current >= source_end;
    }

    inline UInt64 count() const
    {
        return (source_current - source_begin) * 8 + bits_count;
    }

private:
    size_t fillBuffer(UInt8 to_read = BIT_BUFFER_SIZE / 8)
    {
        const size_t available = source_end - source_current;
        if (available == 0)
        {
            return 0;
//            throw Exception("Buffer is empty, but requested to read "
//                            + std::to_string(to_read) + " more bytes.",
//                            ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
        }
        to_read = std::min(static_cast<size_t>(to_read), available);

        memcpy(&bits_buffer, source_current, to_read);
        source_current += to_read;

        bits_buffer = be64toh(bits_buffer);
        bits_buffer >>= BIT_BUFFER_SIZE - to_read * 8;

        bits_count = static_cast<UInt8>(to_read) * 8;

        return to_read;
    }
};

class BitWriter
{
    using BufferType = UInt64;

    char * dest_begin;
    char * dest_current;
    char * dest_end;

    BufferType bits_buffer;
    UInt8 bits_count;

    static constexpr UInt8 BIT_BUFFER_SIZE = sizeof(bits_buffer) * 8;

public:
    BitWriter(char * begin, size_t size)
        : dest_begin(begin),
          dest_current(begin),
          dest_end(begin + size),
          bits_buffer(0),
          bits_count(0)
    {}

    ~BitWriter()
    {
        flush();
    }

    inline void writeBits(UInt8 bits, UInt64 value)
    {
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

    inline UInt64 count() const
    {
        return (dest_current - dest_begin) * 8 + bits_count;
    }

private:
    void doFlush()
    {
        bits_buffer = htobe64(bits_buffer);
        const size_t available = dest_end - dest_current;
        const size_t to_write = (bits_count + 7) / 8;

        if (available < to_write)
        {
            throw Exception("Can not write past end of buffer. Space available "
                            + std::to_string(available) + " bytes, required to write: "
                            + std::to_string(to_write) + ".",
                            ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);
        }

        memcpy(dest_current, &bits_buffer, to_write);
        dest_current += to_write;

        bits_count = 0;
        bits_buffer = 0;
    }
};

}
