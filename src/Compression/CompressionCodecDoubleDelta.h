#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

/** DoubleDelta column codec implementation.
 *
 * Based on Gorilla paper: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf, which was extended
 * to support 64bit types. The drawback is 1 extra bit for 32-byte wide deltas: 5-bit prefix
 * instead of 4-bit prefix.
 *
 * This codec is best used against monotonic integer sequences with constant (or almost constant)
 * stride, like event timestamp for some monitoring application.
 *
 * Given input sequence a: [a0, a1, ... an]:
 *
 * First, write number of items (sizeof(int32)*8 bits):                n
 * Then write first item as is (sizeof(a[0])*8 bits):                  a[0]
 * Second item is written as delta (sizeof(a[0])*8 bits):              a[1] - a[0]
 * Loop over remaining items and calculate double delta:
 *   double_delta = a[i] - 2 * a[i - 1] + a[i - 2]
 *   Write it in compact binary form with `BitWriter`
 *   if double_delta == 0:
 *      write 1bit:                                                    0
 *   else if -63 < double_delta < 64:
 *      write 2 bit prefix:                                            10
 *      write sign bit (1 if signed):                                  x
 *      write 7-1 bits of abs(double_delta - 1):                       xxxxxx
 *   else if -255 < double_delta < 256:
 *      write 3 bit prefix:                                            110
 *      write sign bit (1 if signed):                                  x
 *      write 9-1 bits of abs(double_delta - 1):                       xxxxxxxx
 *   else if -2047 < double_delta < 2048:
 *      write 4 bit prefix:                                            1110
 *      write sign bit (1 if signed):                                  x
 *      write 12-1 bits of abs(double_delta - 1):                      xxxxxxxxxxx
 *   else if double_delta fits into 32-bit int:
 *      write 5 bit prefix:                                            11110
 *      write sign bit (1 if signed):                                  x
 *      write 32-1 bits of abs(double_delta - 1):                      xxxxxxxxxxx...
 *   else
 *      write 5 bit prefix:                                            11111
 *      write sign bit (1 if signed):                                  x
 *      write 64-1 bits of abs(double_delta - 1):                      xxxxxxxxxxx...
 *
 * @example sequence of UInt8 values [1, 2, 3, 4, 5, 6, 7, 8, 9 10] is encoded as (codec header is omitted):
 *
 * .- 4-byte little-endian sequence length (10 == 0xa)
 * |               .- 1 byte (sizeof(UInt8) a[0]                                            : 0x01
 * |               |   .- 1 byte of delta: a[1] - a[0] = 2 - 1 = 1                          : 0x01
 * |               |   |   .- 8 zero bits since double delta for remaining 8 elements was 0 : 0x00
 * v_______________v___v___v___
 * \x0a\x00\x00\x00\x01\x01\x00
 *
 * @example sequence of Int16 values [-10, 10, -20, 20, -40, 40] is encoded as:
 *
 * .- 4-byte little endian sequence length = 6                                 : 0x00000006
 * |                .- 2 bytes (sizeof(Int16) a[0] as UInt16 = -10             : 0xfff6
 * |                |       .- 2 bytes of delta: a[1] - a[0] = 10 - (-10) = 20 : 0x0014
 * |                |       |       .- 4 encoded double deltas (see below)
 * v_______________ v______ v______ v______________________
 * \x06\x00\x00\x00\xf6\xff\x14\x00\xb8\xe2\x2e\xb1\xe4\x58
 *
 * 4 binary encoded double deltas (\xb8\xe2\x2e\xb1\xe4\x58):
 * double_delta (DD) = -20 - 2 * 10 + (-10) = -50
 * .- 2-bit prefix                                                         : 0b10
 * | .- sign-bit                                                           : 0b1
 * | |.- abs(DD - 1) = 49                                                  : 0b110001
 * | ||
 * | ||      DD = 20 - 2 * (-20) + 10 = 70
 * | ||      .- 3-bit prefix                                               : 0b110
 * | ||      |  .- sign bit                                                : 0b0
 * | ||      |  |.- abs(DD - 1) = 69                                       : 0b1000101
 * | ||      |  ||
 * | ||      |  ||        DD = -40 - 2 * 20 + (-20) = -100
 * | ||      |  ||        .- 3-bit prefix                                  : 0b110
 * | ||      |  ||        |    .- sign-bit                                 : 0b0
 * | ||      |  ||        |    |.- abs(DD - 1) = 99                        : 0b1100011
 * | ||      |  ||        |    ||
 * | ||      |  ||        |    ||       DD = 40 - 2 * (-40) + 20 = 140
 * | ||      |  ||        |    ||       .- 3-bit prefix                    : 0b110
 * | ||      |  ||        |    ||       |  .- sign bit                     : 0b0
 * | ||      |  ||        |    ||       |  |.- abs(DD - 1) = 139           : 0b10001011
 * | ||      |  ||        |    ||       |  ||
 * V_vv______V__vv________V____vv_______V__vv________,- padding bits
 * 10111000 11100010 00101110 10110001 11100100 01011000
 *
 * Please also see unit tests for:
 *   * Examples on what output `BitWriter` produces on predefined input.
 *   * Compatibility tests solidifying encoded binary output on set of predefined sequences.
 */
class CompressionCodecDoubleDelta : public ICompressionCodec
{
public:
    CompressionCodecDoubleDelta(UInt8 data_bytes_size_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }

private:
    UInt8 data_bytes_size;
};

}
