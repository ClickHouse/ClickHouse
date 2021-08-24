#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

/** Gorilla column codec implementation.
 *
 * Based on Gorilla paper: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 *
 * This codec is best used against monotonic floating sequences, like CPU usage percentage
 * or any other gauge.
 *
 * Given input sequence a: [a0, a1, ... an]
 *
 * First, write number of items (sizeof(int32)*8 bits):                n
 * Then write first item as is (sizeof(a[0])*8 bits):                  a[0]
 * Loop over remaining items and calculate xor_diff:
 *   xor_diff = a[i] ^ a[i - 1] (e.g. 00000011'10110100)
 *   Write it in compact binary form with `BitWriter`
 *   if xor_diff == 0:
 *       write 1 bit:                                                  0
 *   else:
 *       calculate leading zero bits (lzb)
 *       and trailing zero bits (tzb) of xor_diff,
 *       compare to lzb and tzb of previous xor_diff
 *       (X = sizeof(a[i]) * 8, e.g. X = 16, lzb = 6, tzb = 2)
 *       if lzb >= prev_lzb && tzb >= prev_tzb:
 *           (e.g. prev_lzb=4, prev_tzb=1)
 *           write 2 bit prefix:                                       0b10
 *           write xor_diff >> prev_tzb (X - prev_lzb - prev_tzb bits):0b00111011010
 *           (where X = sizeof(a[i]) * 8, e.g. 16)
 *       else:
 *           write 2 bit prefix:                                       0b11
 *           write 5 bits of lzb:                                      0b00110
 *           write 6 bits of (X - lzb - tzb)=(16-6-2)=8:               0b001000
 *           write (X - lzb - tzb) non-zero bits of xor_diff:          0b11101101
 *           prev_lzb = lzb
 *           prev_tzb = tzb
 *
 * @example sequence of Float32 values [0.1, 0.1, 0.11, 0.2, 0.1] is encoded as:
 *
 * .- 4-byte little endian sequence length: 5                                 : 0x00000005
 * |                .- 4 byte (sizeof(Float32) a[0] as UInt32 : -10           : 0xcdcccc3d
 * |                |               .- 4 encoded xor diffs (see below)
 * v_______________ v______________ v__________________________________________________
 * \x05\x00\x00\x00\xcd\xcc\xcc\x3d\x6a\x5a\xd8\xb6\x3c\xcd\x75\xb1\x6c\x77\x00\x00\x00
 *
 * 4 binary encoded xor diffs (\x6a\x5a\xd8\xb6\x3c\xcd\x75\xb1\x6c\x77\x00\x00\x00):
 *
 * ...........................................
 * a[i-1]   = 00111101110011001100110011001101
 * a[i]     = 00111101110011001100110011001101
 * xor_diff = 00000000000000000000000000000000
 * .- 1-bit prefix                                                           : 0b0
 * |
 * | ...........................................
 * | a[i-1]   = 00111101110011001100110011001101
 * ! a[i]     = 00111101111000010100011110101110
 * | xor_diff = 00000000001011011000101101100011
 * | lzb = 10
 * | tzb = 0
 * |.- 2-bit prefix                                                          : 0b11
 * || .- lzb (10)                                                            : 0b1010
 * || |     .- data length (32-10-0): 22                                     : 0b010110
 * || |     |     .- data                                                    : 0b1011011000101101100011
 * || |     |     |
 * || |     |     |                        ...........................................
 * || |     |     |                        a[i-1]   = 00111101111000010100011110101110
 * || |     |     |                        a[i]     = 00111110010011001100110011001101
 * || |     |     |                        xor_diff = 00000011101011011000101101100011
 * || |     |     |                        .- 2-bit prefix                            : 0b11
 * || |     |     |                        | .- lzb = 6                               : 0b00110
 * || |     |     |                        | |     .- data length = (32 - 6) = 26     : 0b011010
 * || |     |     |                        | |     |      .- data                     : 0b11101011011000101101100011
 * || |     |     |                        | |     |      |
 * || |     |     |                        | |     |      |                            ...........................................
 * || |     |     |                        | |     |      |                            a[i-1]   = 00111110010011001100110011001101
 * || |     |     |                        | |     |      |                            a[i]     = 00111101110011001100110011001101
 * || |     |     |                        | |     |      |                            xor_diff = 00000011100000000000000000000000
 * || |     |     |                        | |     |      |                            .- 2-bit prefix                            : 0b10
 * || |     |     |                        | |     |      |                            | .- data                                  : 0b11100000000000000000000000
 * VV_v____ v_____v________________________V_v_____v______v____________________________V_v_____________________________
 * 01101010 01011010 11011000 10110110 00111100 11001101 01110101 10110001 01101100 01110111 00000000 00000000 00000000
 *
 * Please also see unit tests for:
 *   * Examples on what output `BitWriter` produces on predefined input.
 *   * Compatibility tests solidifying encoded binary output on set of predefined sequences.
 */
class CompressionCodecGorilla : public ICompressionCodec
{
public:
    CompressionCodecGorilla(UInt8 data_bytes_size_);

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
