#pragma once

#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>

#include <bit>
#include <cstring>

/** LSB-first little-endian bit writer, ported from tmp/pcodec_ref/pco/src/bit_writer.rs.
  *
  * Unlike the reference (which ORs into a zero-initialized `Vec`), this writer keeps the current
  * incomplete byte in a register (`partial`) and *overwrites* the output buffer as it goes. It
  * therefore does NOT require the buffer to be zero-initialized — which lets the codec write
  * straight into ClickHouse's destination buffer with no allocation, zeroing, or copy. Padding
  * bits in the final byte of each section are written as zeros explicitly (`finishByte`).
  *
  * The buffer must have at least `MAX_U64S * 8` (16) slack bytes past the logical end, because a
  * single `writeUint` may store up to two 8-byte words starting at the current byte.
  */
namespace DB::Pcodec
{

inline void storeU64LE(uint8_t * p, uint64_t v)
{
    if constexpr (std::endian::native == std::endian::big)
        v = std::byteswap(v);
    std::memcpy(p, &v, sizeof(v));
}

class BitWriter
{
public:
    uint8_t * buf;
    size_t capacity;
    size_t byte_idx = 0;     /// index of the byte holding the current incomplete bits
    uint64_t partial = 0;    /// the `partial_bits` low bits already staged into the current byte
    Bitlen partial_bits = 0; /// number of valid bits in `partial`, always in [0, 8)

    BitWriter(uint8_t * buf_, size_t capacity_) : buf(buf_), capacity(capacity_) { }

    size_t bitIdx() const { return byte_idx * 8 + partial_bits; }

    /// Stages `n` bits of `x` (its lowest `n` bits) into the buffer, advancing the position.
    /// `n` must be <= 64 and `x`'s bits above `n` must be zero (the callers guarantee this).
    template <Latent U>
    void writeUint(U x, Bitlen n)
    {
        uint64_t xv = static_cast<uint64_t>(x);
        uint64_t total = static_cast<uint64_t>(partial_bits) + n;

        // First word: the staged partial bits OR'd with x shifted to the current bit offset.
        uint64_t w0 = partial | (xv << partial_bits);
        storeU64LE(buf + byte_idx, w0);

        Bitlen new_bits = static_cast<Bitlen>(total & 7);
        size_t complete = static_cast<size_t>(total >> 3);

        if (total > 64)
        {
            // x spilled past the first word (only possible when partial_bits > 0).
            uint64_t w1 = xv >> (64 - partial_bits);
            storeU64LE(buf + byte_idx + 8, w1);
            // Leftover (incomplete) bits are in the second word; complete*8 >= 64 here.
            partial = new_bits ? ((w1 >> (complete * 8 - 64)) & ((uint64_t{1} << new_bits) - 1)) : 0;
        }
        else
        {
            // complete*8 == total - new_bits <= 63 when new_bits > 0, so the shift is well-defined.
            partial = new_bits ? ((w0 >> (complete * 8)) & ((uint64_t{1} << new_bits) - 1)) : 0;
        }

        byte_idx += complete;
        partial_bits = new_bits;
    }

    /// Writes `n` bits of a u64 value (for counts and header fields).
    void writeU64(uint64_t x, Bitlen n) { writeUint<uint64_t>(x, n); }

    void writeBool(bool b) { writeUint<uint32_t>(b ? 1u : 0u, 1); }

    void writeAlignedBytes(const uint8_t * bytes, size_t n)
    {
        finishByte();
        std::memcpy(buf + byte_idx, bytes, n);
        byte_idx += n;
    }

    /// Advance to the next byte boundary, writing the current partial byte with zero padding.
    void finishByte()
    {
        if (partial_bits > 0)
        {
            buf[byte_idx] = static_cast<uint8_t>(partial);
            ++byte_idx;
            partial_bits = 0;
            partial = 0;
        }
    }

    /// Total number of complete bytes written so far. Flushes the current partial byte (with zero
    /// padding) into the buffer so the trailing byte is clean, but does not advance the position.
    size_t byteSize() const
    {
        if (partial_bits > 0)
        {
            buf[byte_idx] = static_cast<uint8_t>(partial);
            return byte_idx + 1;
        }
        return byte_idx;
    }
};

}
