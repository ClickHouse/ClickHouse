#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Types.h>

namespace DB
{

/** Reads data from underlying ReadBuffer in bit by bit, max 64 bits at once.
 *
 * reads MSB bits first, imagine that you have a data:
 * 11110000 10101010 00100100 11111111
 *
 * Given that r is BitReader created with a ReadBuffer that reads from data above:
 *  r.readBits(3)  => 0b111
 *  r.readBit()    => 0b1
 *  r.readBits(8)  => 0b1010 // 4 leading zero-bits are not shown
 *  r.readBit()    => 0b1
 *  r.readBit()    => 0b0
 *  r.readBits(16) => 0b100010010011111111
**/

class BitReader
{
    ReadBuffer & buf;

    UInt8 bits_buffer;
    UInt8 bits_count;

public:
    BitReader(ReadBuffer & buf_);
    ~BitReader();

    BitReader(BitReader &&) = default;

    // bits is at most 64
    UInt64 readBits(UInt8 bits);
    UInt8 readBit();

    // true when both bit-buffer and underlying byte-buffer are empty.
    bool eof() const;

private:
    void fillBuffer();
};

class BitWriter
{
    WriteBuffer & buf;

    UInt8 bits_buffer;
    UInt8 bits_count;

public:
    BitWriter(WriteBuffer & buf_);
    ~BitWriter();

    BitWriter(BitWriter &&) = default;

    // write `size` low bits of the `value`.
    void writeBits(UInt8 size, UInt64 value);

    void flush();

private:
    void doFlush();
};

} // namespace DB
