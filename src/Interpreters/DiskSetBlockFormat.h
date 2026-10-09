#pragma once

#include <Compression/PFor.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

#include <array>
#include <optional>
#include <span>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Defines how a set on disk stores its keys in blocks. The keys are distinct and ascending, and `Key` is an
/// unsigned integer of 8 to 256 bits.
///
/// A block packs the gaps between its keys when that pays off, and holds the keys as they are otherwise.
/// Sorted keys that are close to each other have small gaps. Examples are identifiers, dates, and the encoded
/// keys of tuples and of `FixedString` values with a common prefix. A block that packs such gaps with `PFor`
/// holds several times more keys. The set then takes fewer blocks, so lookups read fewer blocks and the
/// directory is smaller.
///
/// A block does not store its first key, because the owner of the blocks keeps that key anyway, as the
/// directory of a set does. The owner passes the first key back to decode the block.
template <typename Key>
class DiskSetBlockFormat
{
public:
    /// A block holds at most this many bytes of keys, packed or not, after a header of a few bytes. A lookup
    /// reads and decodes whole blocks.
    static constexpr size_t block_bytes = 4096;

    /// A block takes at most this many bytes with its header, which records the layout and the number of
    /// keys, and the size of the payload of packed gaps.
    static constexpr size_t max_block_bytes_with_header = sizeof(UInt8) + 2 * sizeof(UInt32) + block_bytes;

    /// Receives the blocks that an `Encoder` writes, in ascending order of their keys. The sink decides how
    /// to store the blocks, so the encoder does not depend on files, frames, or codecs.
    class BlockSink
    {
    public:
        virtual ~BlockSink() = default;

        /// Starts a block whose smallest key is `first_key`. Returns the buffer to write the bytes of the
        /// block into until the next block starts. `compressible` tells the sink whether a general-purpose
        /// codec can shrink the block. The keys of an unpacked block can repeat bytes, but packed gaps are
        /// already dense.
        virtual WriteBuffer & startBlock(const Key & first_key, bool compressible) = 0;
    };

    class Encoder;
    class Decoder;

private:
    /// The first byte of the header of each block records its layout, so that the decoder knows how to read
    /// its keys.
    enum class Layout : UInt8
    {
        /// The block holds the keys as they are.
        Unpacked = 0,
        /// The block holds the offsets of its other keys from its first key, packed by `PFor`. Its header
        /// also records the size of the packed payload.
        PackedGaps = 1,
    };

    static constexpr size_t keys_per_unpacked_block = block_bytes / sizeof(Key);

    /// A block of packed gaps holds at most `max_keys_per_packed_block` keys, which bounds the work of
    /// decoding it for one probe. It holds at least `min_keys_per_packed_block` keys, unless it is the last
    /// block. Decoding packed gaps costs more than copying keys, so packing pays only when it halves the
    /// blocks.
    static constexpr size_t max_keys_per_packed_block = 4096;
    static constexpr size_t min_keys_per_packed_block = 2 * keys_per_unpacked_block;

    /// Keys of at most 32 bits take 32-bit offsets from the first key of their block, because `PFor` packs
    /// 32-bit values with SIMD. Wider keys take 64-bit offsets, so a block of packed gaps of such keys spans
    /// less than 2^64.
    using Offset = std::conditional_t<sizeof(Key) <= sizeof(UInt32), UInt32, UInt64>;

    /// `PFor` encodes the offsets of a block of packed gaps with this delta transform.
    static constexpr PFor::Delta offsets_delta = PFor::Delta::d1;
};

/// Splits keys that ascend strictly into blocks and writes the blocks to a `BlockSink`.
///
/// Each block starts as a block of packed gaps. It takes the keys one at a time while its packed payload fits
/// in `block_bytes`. When a key does not fit, the encoder checks how many keys the block holds. If the block
/// holds at least `min_keys_per_packed_block` keys, the encoder writes it as a block of packed gaps, and the
/// key starts the next block. Otherwise packing does not pay for these keys, so the block becomes an unpacked
/// block, which takes the following keys in bulk until it is full. The block after it starts as a block of
/// packed gaps again.
///
/// The encoder writes a block only when a key arrives that the block cannot take. So the last block stays
/// pending until `finish` writes it.
template <typename Key>
class DiskSetBlockFormat<Key>::Encoder
{
public:
    /// Adds `keys`, which ascend and are greater than the keys added before. Writes the blocks that they
    /// complete to `sink`.
    void add(std::span<const Key> keys, BlockSink & sink);

    /// Writes the pending keys as the last block. Call it once, after the last `add`, which must have added
    /// keys.
    void finish(BlockSink & sink);

private:
    /// Starts the pending block with `key`. A new block tries packing first.
    void startPendingBlock(const Key & key);

    /// Adds `key` to the pending block of packed gaps if it fits, and returns whether it did. It fits when
    /// its offset fits `Offset`, the block is not full, and the packed payload stays within `block_bytes`.
    bool tryAddToPackedBlock(const Key & key);

    /// Adds as many keys from the start of `keys` as the pending unpacked block has room for, and returns
    /// their number.
    size_t addToUnpackedBlock(std::span<const Key> keys);

    /// Returns the offset of `key` from the first pending key, or nothing when it does not fit `Offset`.
    std::optional<Offset> getOffsetInPendingBlock(const Key & key) const;

    /// Writes the first `count` pending keys as the next block, in the layout of the pending block. The other
    /// keys stay pending.
    void writeBlock(size_t count, BlockSink & sink);

    /// Writes the payload of the block of packed gaps that holds `block_keys`.
    void writePackedGaps(std::span<const Key> block_keys, WriteBuffer & out);

    /// These describe the block being filled: its keys, its layout, and the bound of its payload while it is
    /// a block of packed gaps.
    VectorWithMemoryTracking<Key> pending_keys;
    Layout pending_layout = Layout::PackedGaps;
    PFor::EncodedBytesBound<Offset, offsets_delta> payload_bound;

    /// The encoder reuses these buffers for each block of packed gaps, so that it does not allocate them per
    /// block.
    VectorWithMemoryTracking<Offset> offsets;
    VectorWithMemoryTracking<uint8_t> payload;
};

/// Decodes the blocks that an `Encoder` writes. It keeps the keys of the block that it decoded last, so
/// concurrent lookups each use their own decoder.
template <typename Key>
class DiskSetBlockFormat<Key>::Decoder
{
public:
    /// Decodes the block that `in` holds, whose smallest key is `first_key`. Returns its keys, which stay
    /// valid until the next call.
    std::span<const Key> decode(ReadBuffer & in, const Key & first_key);

private:
    /// Decodes the payload of a block of packed gaps from `in` into `keys`. `decode` has already sized `keys`
    /// to the number of keys of the block.
    void decodePackedGaps(ReadBuffer & in, const Key & first_key);

    PODArray<Key> keys;

    /// These hold the payload of the block of packed gaps being decoded and the offsets of its keys. A fixed
    /// buffer can hold the payload, because the encoder keeps every payload within `block_bytes`.
    std::array<uint8_t, block_bytes> payload{};
    PODArray<Offset> offsets;
};

}
