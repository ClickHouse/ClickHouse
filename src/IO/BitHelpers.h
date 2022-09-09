#pragma once

#include <base/types.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>

#include <cstring>
#include <cassert>


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
    using BufferType = unsigned __int128;

    const char * source_begin;
    const char * source_current;
    const char * source_end;

    BufferType bits_buffer;
    UInt8 bits_count;

public:
    BitReader(const char * begin, size_t size)
        : source_begin(begin),
          source_current(begin),
          source_end(begin + size),
          bits_buffer(0),
          bits_count(0)
    {}

    ~BitReader() = default;

    // reads bits_to_read high-bits from bits_buffer
    ALWAYS_INLINE inline UInt64 readBits(UInt8 bits_to_read)
    {
        if (bits_to_read > bits_count)
            fillBitBuffer();

        return getBitsFromBitBuffer<CONSUME>(bits_to_read);
    }

    inline UInt8 peekByte()
    {
        if (bits_count < 8)
            fillBitBuffer();

        return getBitsFromBitBuffer<PEEK>(8);
    }

    ALWAYS_INLINE inline UInt8 readBit()
    {
        return static_cast<UInt8>(readBits(1));
    }

    // skip bits from bits_buffer
    inline void skipBufferedBits(UInt8 bits)
    {
        bits_buffer <<= bits;
        bits_count -= bits;
    }


    inline bool eof() const
    {
        return bits_count == 0 && source_current >= source_end;
    }

    // number of bits that was already read by clients with readBits()
    inline UInt64 count() const
    {
        return (source_current - source_begin) * 8 - bits_count;
    }

    inline UInt64 remaining() const
    {
        return (source_end - source_current) * 8 + bits_count;
    }

private:
    enum GetBitsMode {CONSUME, PEEK};
    // read data from internal buffer, if it has not enough bits, result is undefined.
    template <GetBitsMode mode>
    inline UInt64 getBitsFromBitBuffer(UInt8 bits_to_read)
    {
        assert(bits_to_read > 0);

        // push down the high-bits
        const UInt64 result = static_cast<UInt64>(bits_buffer >> (sizeof(bits_buffer) * 8 - bits_to_read));

        if constexpr (mode == CONSUME)
        {
            // 'erase' high-bits that were have read
            skipBufferedBits(bits_to_read);
        }

        return result;
    }


    // Fills internal bits_buffer with data from source, reads at most 64 bits
    ALWAYS_INLINE size_t fillBitBuffer()
    {
        const size_t available = source_end - source_current;
        const auto bytes_to_read = std::min<size_t>(64 / 8, available);
        if (available == 0)
        {
            if (bytes_to_read == 0)
                return 0;

            throw Exception("Buffer is empty, but requested to read "
                            + std::to_string(bytes_to_read) + " more bytes.",
                            ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
        }

        UInt64 tmp_buffer = 0;
        memcpy(&tmp_buffer, source_current, bytes_to_read);
        source_current += bytes_to_read;

        tmp_buffer = __builtin_bswap64(tmp_buffer);

        bits_buffer |= BufferType(tmp_buffer) << ((sizeof(BufferType) - sizeof(tmp_buffer)) * 8 - bits_count);
        bits_count += static_cast<UInt8>(bytes_to_read) * 8;

        return bytes_to_read;
    }
};

class BitWriter
{
    using BufferType = unsigned __int128;

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

    // write `bits_to_write` low-bits of `value` to the buffer
    inline void writeBits(UInt8 bits_to_write, UInt64 value)
    {
        assert(bits_to_write > 0);

        UInt32 capacity = BIT_BUFFER_SIZE - bits_count;
        if (capacity < bits_to_write)
        {
            doFlush();
            capacity = BIT_BUFFER_SIZE - bits_count;
        }

        // write low bits of value as high bits of bits_buffer
        const UInt64 mask = maskLowBits<UInt64>(bits_to_write);
        BufferType v = value & mask;
        v <<= capacity - bits_to_write;

        bits_buffer |= v;
        bits_count += bits_to_write;
    }

    // flush contents of bits_buffer to the dest_current, partial bytes are completed with zeroes.
    inline void flush()
    {
        bits_count = (bits_count + 8 - 1) & ~(8 - 1); // align up to 8-bytes, so doFlush will write all data from bits_buffer
        while (bits_count != 0)
            doFlush();
    }

    inline UInt64 count() const
    {
        return (dest_current - dest_begin) * 8 + bits_count;
    }

private:
    void doFlush()
    {
        // write whole bytes to the dest_current, leaving partial bits in bits_buffer
        const size_t available = dest_end - dest_current;
        const size_t to_write = std::min<size_t>(sizeof(UInt64), bits_count / 8); // align to 8-bit boundary

        if (available < to_write)
        {
            throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER,
                "Can not write past end of buffer. Space available {} bytes, required to write {} bytes.",
                available, to_write);
        }

        const auto tmp_buffer = __builtin_bswap64(static_cast<UInt64>(bits_buffer >> (sizeof(bits_buffer) - sizeof(UInt64)) * 8));
        memcpy(dest_current, &tmp_buffer, to_write);
        dest_current += to_write;

        bits_buffer <<= to_write * 8;
        bits_count -= to_write * 8;
    }
};

}
