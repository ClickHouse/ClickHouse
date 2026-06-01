#pragma once

#include <Compression/Pcodec/Bits.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/PcodecError.h>

#include <bit>
#include <cstring>

/** LSB-first little-endian bit reader, ported from tmp/pcodec_ref/pco/src/bit_reader.rs.
  *
  * NOTE: this is intentionally NOT ClickHouse's `BitReader` from IO/BitHelpers.h, which is
  * MSB-first. pcodec packs bits LSB-first within each byte and little-endian across bytes; the
  * exact bit order is part of the wire format.
  *
  * Unlike the reference, this reader operates on a single contiguous in-memory buffer (the whole
  * compressed block is available at once). The buffer MUST have at least `OVERSHOOT_PADDING`
  * readable slack bytes after `unpadded_byte_size`, because the reader may load a full `u64` (and,
  * for the two-word path, a bit more) past the logical position.
  */
namespace DB::Pcodec
{

/// Read 8 bytes at `p` as a little-endian u64 (unaligned-safe).
inline uint64_t loadU64LE(const uint8_t * p)
{
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
    if constexpr (std::endian::native == std::endian::big)
        v = std::byteswap(v);
    return v;
}

/// Read 4 bytes at `p` as a little-endian u32 (unaligned-safe).
inline uint32_t loadU32LE(const uint8_t * p)
{
    uint32_t v;
    std::memcpy(&v, p, sizeof(v));
    if constexpr (std::endian::native == std::endian::big)
        v = std::byteswap(v);
    return v;
}

/// Free-standing positional reads, used by the offset hot loop (bit_reader.rs::read_uint_at).
/// `read_bytes` selects the access width: 4 (<=25 bits), 8 (<=57 bits), 15 (<=113 bits).
template <Latent U, size_t read_bytes>
inline U readUintAt(const uint8_t * src, size_t byte_idx, Bitlen bits_past, Bitlen n)
{
    if constexpr (read_bytes == 4)
        return static_cast<U>(lowestBitsFast<uint32_t>(loadU32LE(src + byte_idx) >> bits_past, n));
    else if constexpr (read_bytes == 8)
        return static_cast<U>(lowestBitsFast<uint64_t>(loadU64LE(src + byte_idx) >> bits_past, n));
    else
    {
        uint64_t first_word = loadU64LE(src + byte_idx) >> bits_past;
        Bitlen processed = 56 - bits_past;
        uint64_t second_word = loadU64LE(src + byte_idx + 7) << processed;
        return static_cast<U>(lowestBits<uint64_t>(first_word | second_word, n));
    }
}

class BitReader
{
public:
    const uint8_t * src;
    size_t unpadded_bit_size;
    size_t stale_byte_idx = 0;
    Bitlen bits_past_byte = 0;

    BitReader(const uint8_t * src_, size_t unpadded_byte_size_, Bitlen bits_past_byte_)
        : src(src_), unpadded_bit_size(unpadded_byte_size_ * 8), bits_past_byte(bits_past_byte_)
    {
    }

    size_t bitIdx() const { return stale_byte_idx * 8 + bits_past_byte; }

    size_t byteIdx() const { return bitIdx() / 8; }

    /// Reads `n` bits (0 <= n <= U::BITS) as a latent of type U, advancing the position.
    template <Latent U>
    U readUint(Bitlen n)
    {
        refill();
        U res;
        if constexpr (maxBytesFor<U> <= 4)
            res = readU32At<U>(stale_byte_idx, bits_past_byte, n);
        else if constexpr (maxBytesFor<U> <= 8)
            res = readU64At<U>(stale_byte_idx, bits_past_byte, n);
        else
            res = readAlmostU64x2At<U>(stale_byte_idx, bits_past_byte, n);
        consume(n);
        return res;
    }

    /// Reads `n` bits into a u64 (used for counts and header fields). Uses the two-word path.
    uint64_t readU64(Bitlen n) { return readUint<uint64_t>(n); }

    bool readBool()
    {
        refill();
        bool res = (src[stale_byte_idx] & (1u << bits_past_byte)) != 0;
        consume(1);
        return res;
    }

    /// Reads `n` bytes from a byte-aligned position, returning a pointer into the source.
    const uint8_t * readAlignedBytes(size_t n)
    {
        size_t byte_idx = alignedByteIdx();
        size_t new_byte_idx = byte_idx + n;
        stale_byte_idx = new_byte_idx;
        bits_past_byte = 0;
        return src + byte_idx;
    }

    /// Seek to the end of the current byte, requiring all skipped bits to be zero. Used to
    /// terminate each section of the stream, which always starts and ends byte-aligned.
    void drainEmptyByte(const char * message)
    {
        checkInBounds();
        refill();
        if (bits_past_byte != 0)
        {
            if ((src[stale_byte_idx] >> bits_past_byte) > 0)
                throw PcodecError(message);
            consume(8 - bits_past_byte);
        }
    }

    void checkInBounds() const
    {
        if (bitIdx() > unpadded_bit_size)
            throw PcodecError("pcodec: out of bounds while reading");
    }

    size_t alignedByteIdx() const
    {
        if (bits_past_byte % 8 != 0)
            throw PcodecError("pcodec: cannot get aligned byte index on misaligned bit reader");
        return byteIdx();
    }

private:
    void refill()
    {
        stale_byte_idx += bits_past_byte / 8;
        bits_past_byte %= 8;
    }

    void consume(Bitlen n) { bits_past_byte += n; }

    template <Latent U>
    U readU32At(size_t byte_idx, Bitlen bits_past, Bitlen n) const
    {
        return static_cast<U>(lowestBitsFast<uint32_t>(loadU32LE(src + byte_idx) >> bits_past, n));
    }

    template <Latent U>
    U readU64At(size_t byte_idx, Bitlen bits_past, Bitlen n) const
    {
        return static_cast<U>(lowestBitsFast<uint64_t>(loadU64LE(src + byte_idx) >> bits_past, n));
    }

    template <Latent U>
    U readAlmostU64x2At(size_t byte_idx, Bitlen bits_past, Bitlen n) const
    {
        uint64_t first_word = loadU64LE(src + byte_idx) >> bits_past;
        Bitlen processed = 56 - bits_past;
        uint64_t second_word = loadU64LE(src + byte_idx + 7) << processed;
        return static_cast<U>(lowestBits<uint64_t>(first_word | second_word, n));
    }
};

}
